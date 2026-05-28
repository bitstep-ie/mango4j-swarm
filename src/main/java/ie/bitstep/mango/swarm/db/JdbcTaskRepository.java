package ie.bitstep.mango.swarm.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import ie.bitstep.mango.swarm.TaskStatus;
import ie.bitstep.mango.swarm.UuidV7;

/**
 * JDBC/PostgreSQL implementation of {@link TaskRepository}.
 *
 * <p>Uses SQL row locking and task-state updates to support safe multi-worker claiming, progress tracking, retry
 * rescheduling, timeout recovery, and retention cleanup.
 */
public class JdbcTaskRepository implements TaskRepository {
	private static final String INSERT_TASK_SQL =
			"""
			INSERT INTO mango_swarm_tasks(id, task_type, payload, status, available_at)
			VALUES (?, ?, ?, 'queued', ?)
			""";
	private static final String INSERT_PACER_SQL =
			"""
			INSERT INTO mango_swarm_task_pacers(task_type, slot_at, task_id)
			VALUES (?, ?, ?)
			""";
	private static final String FIND_SLOT_BEFORE_SQL =
			"""
			SELECT slot_at
			FROM mango_swarm_task_pacers
			WHERE task_type = ?
			AND slot_at <= ?
			ORDER BY slot_at DESC
			LIMIT 1
			""";
	private static final String FIND_SLOT_AFTER_SQL =
			"""
			SELECT slot_at
			FROM mango_swarm_task_pacers
			WHERE task_type = ?
			AND slot_at > ?
			ORDER BY slot_at ASC
			LIMIT 1
			""";
	private static final String SELECT_QUEUED_TASK_IDS_SQL =
			"""
			SELECT id
			FROM mango_swarm_tasks
			WHERE task_type = ?
			AND status = 'queued'
			AND available_at <= ?
			ORDER BY available_at, id
			LIMIT ?
			""";
	private static final String CLAIM_TASK_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'claimed',
				claimed_by = ?,
				claimed_at = ?,
				progress_percent = NULL,
				progress_description = NULL,
				last_progress_at = NULL,
				attempt_count = attempt_count + 1,
				updated_at = ?
			WHERE id = ?
			AND status = 'queued'
			""";
	private static final String SELECT_CLAIMED_TASK_SQL =
			"""
			SELECT id, task_type, payload, status, available_at, claimed_by, claimed_at,
					attempt_count, created_at, updated_at
			FROM mango_swarm_tasks
			WHERE id = ?
			AND claimed_by = ?
			""";
	private static final String MARK_IN_PROGRESS_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'in_progress', last_progress_at = ?, updated_at = ?
			WHERE id = ? AND claimed_by = ? AND status = 'claimed'
			""";
	private static final String RECORD_PROGRESS_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET progress_percent = ?, progress_description = ?, last_progress_at = ?, updated_at = ?
			WHERE id = ? AND claimed_by = ? AND status = 'in_progress'
			""";
	private static final String MARK_COMPLETED_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'completed',
				completed_at = ?,
				progress_percent = 100,
				progress_description = 'finished',
				last_progress_at = ?,
				updated_at = ?
			WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
			""";
	private static final String MARK_FAILED_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'failed', failed_at = ?, updated_at = ?, last_error_message = ?
			WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
			""";
	private static final String RESCHEDULE_AFTER_FAILURE_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'queued',
				available_at = ?,
				claimed_by = NULL,
				claimed_at = NULL,
				progress_percent = NULL,
				progress_description = NULL,
				last_progress_at = NULL,
				failed_at = NULL,
				updated_at = ?,
				last_error_message = ?
			WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
			""";
	private static final String REQUEUE_CLAIMED_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'queued',
				available_at = ?,
				claimed_by = NULL,
				claimed_at = NULL,
				progress_percent = NULL,
				progress_description = NULL,
				last_progress_at = NULL,
				updated_at = ?,
				last_error_message = ?
			WHERE id = ? AND claimed_by = ? AND status = 'claimed'
			""";
	private static final String RECLAIM_TIMED_OUT_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'queued',
				claimed_by = NULL,
				claimed_at = NULL,
				progress_percent = NULL,
				progress_description = NULL,
				last_progress_at = NULL,
				updated_at = ?,
				last_error_message = 'Reclaimed after timeout'
			WHERE id IN (
				SELECT id
				FROM mango_swarm_tasks
				WHERE task_type = ?
				AND status IN ('claimed', 'in_progress')
				AND COALESCE(last_progress_at, claimed_at) < ?
				ORDER BY COALESCE(last_progress_at, claimed_at), id
				LIMIT ?
			)
			""";
	private static final String MARK_TIMED_OUT_FAILED_SQL =
			"""
			UPDATE mango_swarm_tasks
			SET status = 'failed',
				failed_at = ?,
				updated_at = ?,
				last_error_message = 'Task timed out and reclaim is disabled'
			WHERE id IN (
				SELECT id
				FROM mango_swarm_tasks
				WHERE task_type = ?
				AND status IN ('claimed', 'in_progress')
				AND COALESCE(last_progress_at, claimed_at) < ?
				ORDER BY COALESCE(last_progress_at, claimed_at), id
				LIMIT ?
			)
			""";
	private static final String DELETE_COMPLETED_SQL =
			"""
			DELETE FROM mango_swarm_tasks
			WHERE id IN (
				SELECT id
				FROM mango_swarm_tasks
				WHERE status = 'completed'
				AND completed_at IS NOT NULL
				AND completed_at < ?
				ORDER BY completed_at, id
				LIMIT ?
			)
			""";
	private static final String DELETE_FAILED_SQL =
			"""
			DELETE FROM mango_swarm_tasks
			WHERE id IN (
				SELECT id
				FROM mango_swarm_tasks
				WHERE status = 'failed'
				AND failed_at IS NOT NULL
				AND failed_at < ?
				ORDER BY failed_at, id
				LIMIT ?
			)
			""";
	private static final String DELETE_PACERS_SQL =
			"""
			DELETE FROM mango_swarm_task_pacers
			WHERE (task_type, slot_at) IN (
				SELECT task_type, slot_at
				FROM mango_swarm_task_pacers
				WHERE slot_at < ?
				ORDER BY slot_at, task_type
				LIMIT ?
			)
			""";

	private final JdbcTemplate jdbcTemplate;
	private final ObjectMapper objectMapper;
	private final SchemaQualifiedTables tables;
	private final RowMapper<TaskRecord> rowMapper = this::mapTask;
	private Boolean h2;

	public JdbcTaskRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
		this(jdbcTemplate, objectMapper, new SchemaQualifiedTables(null));
	}

	public JdbcTaskRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, SchemaQualifiedTables tables) {
		this.jdbcTemplate = jdbcTemplate;
		this.objectMapper = objectMapper;
		this.tables = tables;
	}

	@Override
	public UUID queue(String taskType, JsonNode payload, Instant availableAt) {
		return jdbcTemplate.execute(
				(ConnectionCallback<UUID>) connection -> tables.withSearchPath(connection, scoped -> {
					UUID taskId = UuidV7.generate();
					try (PreparedStatement statement = scoped.prepareStatement(INSERT_TASK_SQL)) {
						statement.setObject(1, taskId);
						statement.setString(2, taskType);
						setJson(statement, 3, payload);
						statement.setTimestamp(4, Timestamp.from(availableAt));
						statement.executeUpdate();
						return taskId;
					}
				}));
	}

	@Override
	public synchronized UUID queueInNextSlot(
			String taskType, JsonNode payload, Instant requestedAt, Duration slotSpacing) {
		if (slotSpacing == null || slotSpacing.isZero() || slotSpacing.isNegative()) {
			return queue(taskType, payload, requestedAt);
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<UUID>) connection -> tables.withSearchPath(connection, scoped -> {
					UUID taskId = UuidV7.generate();
					Instant availableAt = reserveSlot(scoped, taskType, taskId, requestedAt, slotSpacing);
					try (PreparedStatement statement = scoped.prepareStatement(INSERT_TASK_SQL)) {
						statement.setObject(1, taskId);
						statement.setString(2, taskType);
						setJson(statement, 3, payload);
						statement.setTimestamp(4, Timestamp.from(availableAt));
						statement.executeUpdate();
						return taskId;
					}
				}));
	}

	private Instant reserveSlot(
			java.sql.Connection connection, String taskType, UUID taskId, Instant requestedAt, Duration slotSpacing)
			throws SQLException {
		Instant candidate = requestedAt;
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts > 50_000) {
				throw new SQLException("Unable to reserve slot after " + attempts + " attempts for taskType=" + taskType
						+ " requestedAt=" + requestedAt);
			}
			Instant before = findSlot(connection, taskType, candidate, true);
			if (before != null && candidate.isBefore(before.plus(slotSpacing))) {
				candidate = before.plus(slotSpacing);
				continue;
			}
			Instant after = findSlot(connection, taskType, candidate, false);
			if (after != null && candidate.plus(slotSpacing).isAfter(after)) {
				candidate = after.plus(slotSpacing);
				continue;
			}
			try (PreparedStatement insert = connection.prepareStatement(INSERT_PACER_SQL)) {
				insert.setString(1, taskType);
				insert.setTimestamp(2, Timestamp.from(candidate));
				insert.setObject(3, taskId);
				insert.executeUpdate();
			} catch (SQLIntegrityConstraintViolationException ex) {
				candidate = candidate.plus(slotSpacing);
				continue;
			}
			return candidate;
		}
	}

	private Instant findSlot(java.sql.Connection connection, String taskType, Instant candidate, boolean before)
			throws SQLException {
		if (before) {
			return findSlotBefore(connection, taskType, candidate);
		}
		return findSlotAfter(connection, taskType, candidate);
	}

	private Instant findSlotBefore(java.sql.Connection connection, String taskType, Instant candidate)
			throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(FIND_SLOT_BEFORE_SQL)) {
			statement.setString(1, taskType);
			statement.setTimestamp(2, Timestamp.from(candidate));
			try (ResultSet rs = statement.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				return rs.getTimestamp(1).toInstant();
			}
		}
	}

	private Instant findSlotAfter(java.sql.Connection connection, String taskType, Instant candidate)
			throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(FIND_SLOT_AFTER_SQL)) {
			statement.setString(1, taskType);
			statement.setTimestamp(2, Timestamp.from(candidate));
			try (ResultSet rs = statement.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				return rs.getTimestamp(1).toInstant();
			}
		}
	}

	@Override
	public List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit) {
		if (limit < 1) {
			return List.of();
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<List<TaskRecord>>) connection -> tables.withSearchPath(connection, scoped -> {
					List<UUID> ids = new ArrayList<>();
					try (PreparedStatement select = scoped.prepareStatement(SELECT_QUEUED_TASK_IDS_SQL)) {
						select.setString(1, taskType);
						select.setTimestamp(2, Timestamp.from(now));
						select.setInt(3, limit);
						try (ResultSet rs = select.executeQuery()) {
							while (rs.next()) {
								ids.add(rs.getObject("id", UUID.class));
							}
						}
					}
					if (ids.isEmpty()) {
						return List.of();
					}
					try (PreparedStatement update = scoped.prepareStatement(CLAIM_TASK_SQL)) {
						for (UUID id : ids) {
							update.setObject(1, workerId);
							update.setTimestamp(2, Timestamp.from(now));
							update.setTimestamp(3, Timestamp.from(now));
							update.setObject(4, id);
							update.executeUpdate();
						}
					}
					List<TaskRecord> claimed = new ArrayList<>();
					try (PreparedStatement query = scoped.prepareStatement(SELECT_CLAIMED_TASK_SQL)) {
						for (UUID id : ids) {
							query.setObject(1, id);
							query.setObject(2, workerId);
							try (ResultSet rs = query.executeQuery()) {
								if (rs.next()) {
									claimed.add(mapTask(rs, claimed.size()));
								}
							}
						}
					}
					return claimed;
				}));
	}

	@Override
	public void markInProgress(UUID taskId, UUID workerId, Instant now) {
		jdbcTemplate.execute((ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
			try (PreparedStatement statement = scoped.prepareStatement(MARK_IN_PROGRESS_SQL)) {
				statement.setTimestamp(1, Timestamp.from(now));
				statement.setTimestamp(2, Timestamp.from(now));
				statement.setObject(3, taskId);
				statement.setObject(4, workerId);
				return statement.executeUpdate();
			}
		}));
	}

	@Override
	public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {
		jdbcTemplate.execute((ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
			try (PreparedStatement statement = scoped.prepareStatement(RECORD_PROGRESS_SQL)) {
				statement.setInt(1, progressPercent);
				statement.setString(2, truncate(description));
				statement.setTimestamp(3, Timestamp.from(now));
				statement.setTimestamp(4, Timestamp.from(now));
				statement.setObject(5, taskId);
				statement.setObject(6, workerId);
				return statement.executeUpdate();
			}
		}));
	}

	@Override
	public void markCompleted(UUID taskId, UUID workerId, Instant now) {
		jdbcTemplate.execute((ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
			try (PreparedStatement statement = scoped.prepareStatement(MARK_COMPLETED_SQL)) {
				statement.setTimestamp(1, Timestamp.from(now));
				statement.setTimestamp(2, Timestamp.from(now));
				statement.setTimestamp(3, Timestamp.from(now));
				statement.setObject(4, taskId);
				statement.setObject(5, workerId);
				return statement.executeUpdate();
			}
		}));
	}

	@Override
	public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
		jdbcTemplate.execute((ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
			try (PreparedStatement statement = scoped.prepareStatement(MARK_FAILED_SQL)) {
				statement.setTimestamp(1, Timestamp.from(now));
				statement.setTimestamp(2, Timestamp.from(now));
				statement.setString(3, truncate(errorMessage));
				statement.setObject(4, taskId);
				statement.setObject(5, workerId);
				return statement.executeUpdate();
			}
		}));
	}

	@Override
	public void rescheduleAfterFailure(
			UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage) {
		jdbcTemplate.execute((ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
			try (PreparedStatement statement = scoped.prepareStatement(RESCHEDULE_AFTER_FAILURE_SQL)) {
				statement.setTimestamp(1, Timestamp.from(availableAt));
				statement.setTimestamp(2, Timestamp.from(now));
				statement.setString(3, truncate(errorMessage));
				statement.setObject(4, taskId);
				statement.setObject(5, workerId);
				return statement.executeUpdate();
			}
		}));
	}

	@Override
	public void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason) {
		jdbcTemplate.execute((ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
			try (PreparedStatement statement = scoped.prepareStatement(REQUEUE_CLAIMED_SQL)) {
				statement.setTimestamp(1, Timestamp.from(availableAt));
				statement.setTimestamp(2, Timestamp.from(now));
				statement.setString(3, truncate(reason));
				statement.setObject(4, taskId);
				statement.setObject(5, workerId);
				return statement.executeUpdate();
			}
		}));
	}

	@Override
	public int reclaimTimedOut(String taskType, Duration timeout, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(RECLAIM_TIMED_OUT_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setString(2, taskType);
						statement.setTimestamp(3, Timestamp.from(now.minus(timeout)));
						statement.setInt(4, limit);
						return statement.executeUpdate();
					}
				}));
	}

	@Override
	public int markTimedOutFailed(String taskType, Duration timeout, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(MARK_TIMED_OUT_FAILED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setString(3, taskType);
						statement.setTimestamp(4, Timestamp.from(now.minus(timeout)));
						statement.setInt(5, limit);
						return statement.executeUpdate();
					}
				}));
	}

	@Override
	public int deleteCompletedOlderThan(Duration retention, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_COMPLETED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}));
	}

	@Override
	public int deleteFailedOlderThan(Duration retention, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_FAILED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}));
	}

	@Override
	public int deleteTaskPacersOlderThan(Duration retention, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return jdbcTemplate.execute(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_PACERS_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}));
	}

	private void setJson(PreparedStatement statement, int parameterIndex, JsonNode payload) throws SQLException {
		if (isH2()) {
			statement.setString(parameterIndex, json(payload));
			return;
		}
		PGobject object = new PGobject();
		object.setType("jsonb");
		object.setValue(json(payload));
		statement.setObject(parameterIndex, object, Types.OTHER);
	}

	private String json(JsonNode payload) throws SQLException {
		try {
			return objectMapper.writeValueAsString(payload);
		} catch (JsonProcessingException ex) {
			throw new SQLException("Cannot serialize task payload", ex);
		}
	}

	TaskRecord mapTask(ResultSet rs, int rowNum) throws SQLException {
		try {
			Timestamp claimedAt = rs.getTimestamp("claimed_at");
			return new TaskRecord(
					rs.getObject("id", UUID.class),
					rs.getString("task_type"),
					objectMapper.readTree(rs.getString("payload")),
					TaskStatus.valueOf(rs.getString("status")),
					rs.getTimestamp("available_at").toInstant(),
					rs.getObject("claimed_by", UUID.class),
					claimedAt == null ? null : claimedAt.toInstant(),
					rs.getInt("attempt_count"),
					rs.getTimestamp("created_at").toInstant(),
					rs.getTimestamp("updated_at").toInstant());
		} catch (JsonProcessingException ex) {
			throw new SQLException("Cannot parse task payload", ex);
		}
	}

	private static String truncate(String message) {
		if (message == null) {
			return null;
		}
		return message.length() > 4000 ? message.substring(0, 4000) : message;
	}

	private boolean isH2() {
		if (h2 == null) {
			h2 = jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
				String productName = connection.getMetaData().getDatabaseProductName();
				return productName != null && productName.toLowerCase().contains("h2");
			});
		}
		return h2;
	}
}
