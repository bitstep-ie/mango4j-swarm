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
import java.util.Collections;
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
		return jdbcTemplate.execute((ConnectionCallback<UUID>) connection -> {
			UUID taskId = UuidV7.generate();
			try (PreparedStatement statement = connection.prepareStatement(
					"""
					INSERT INTO %s(id, task_type, payload, status, available_at)
					VALUES (?, ?, ?, 'queued', ?)
					"""
							.formatted(tables.tasks()))) {
				statement.setObject(1, taskId);
				statement.setString(2, taskType);
				setJson(statement, 3, payload);
				statement.setTimestamp(4, Timestamp.from(availableAt));
				statement.executeUpdate();
				return taskId;
			}
		});
	}

	@Override
	public synchronized UUID queueInNextSlot(
			String taskType, JsonNode payload, Instant requestedAt, Duration slotSpacing) {
		if (slotSpacing == null || slotSpacing.isZero() || slotSpacing.isNegative()) {
			return queue(taskType, payload, requestedAt);
		}
		return jdbcTemplate.execute((ConnectionCallback<UUID>) connection -> {
			try {
				UUID taskId = UuidV7.generate();
				Instant availableAt = reserveSlot(connection, taskType, taskId, requestedAt, slotSpacing);
				try (PreparedStatement statement = connection.prepareStatement(
						"""
						INSERT INTO %s(id, task_type, payload, status, available_at)
						VALUES (?, ?, ?, 'queued', ?)
						"""
								.formatted(tables.tasks()))) {
					statement.setObject(1, taskId);
					statement.setString(2, taskType);
					setJson(statement, 3, payload);
					statement.setTimestamp(4, Timestamp.from(availableAt));
					statement.executeUpdate();
					return taskId;
				}
			} catch (Exception ex) {
				throw ex;
			}
		});
	}

	private Instant reserveSlot(
			java.sql.Connection connection, String taskType, UUID taskId, Instant requestedAt, Duration slotSpacing)
			throws SQLException {
		Instant candidate = requestedAt;
		int attempts = 0;
		while (true) {
			attempts++;
			if (attempts > 50_000) {
				throw new SQLException("Unable to reserve slot after %d attempts for taskType=%s requestedAt=%s"
						.formatted(attempts, taskType, requestedAt));
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
			try (PreparedStatement insert = connection.prepareStatement(
					"""
					INSERT INTO %s(task_type, slot_at, task_id)
					VALUES (?, ?, ?)
					"""
							.formatted(tables.taskPacers()))) {
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
		String comparison = before ? "<=" : ">";
		String ordering = before ? "DESC" : "ASC";
		try (PreparedStatement statement = connection.prepareStatement(
				"""
				SELECT slot_at
				FROM %s
				WHERE task_type = ?
				AND slot_at %s ?
				ORDER BY slot_at %s
				LIMIT 1
				"""
						.formatted(tables.taskPacers(), comparison, ordering))) {
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
		return jdbcTemplate.execute((ConnectionCallback<List<TaskRecord>>) connection -> {
			List<UUID> ids = new ArrayList<>();
			try (PreparedStatement select = connection.prepareStatement(
					"""
					SELECT id
					FROM %s
					WHERE task_type = ?
					AND status = 'queued'
					AND available_at <= ?
					ORDER BY available_at, id
					LIMIT ?
					"""
							.formatted(tables.tasks()))) {
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
			String placeholders = String.join(", ", Collections.nCopies(ids.size(), "?"));
			try (PreparedStatement update = connection.prepareStatement(
					"""
					UPDATE %s
					SET status = 'claimed',
						claimed_by = ?,
						claimed_at = ?,
						progress_percent = NULL,
						progress_description = NULL,
						last_progress_at = NULL,
						attempt_count = attempt_count + 1,
						updated_at = ?
					WHERE id IN (%s)
					AND status = 'queued'
					"""
							.formatted(tables.tasks(), placeholders))) {
				update.setObject(1, workerId);
				update.setTimestamp(2, Timestamp.from(now));
				update.setTimestamp(3, Timestamp.from(now));
				for (int i = 0; i < ids.size(); i++) {
					update.setObject(i + 4, ids.get(i));
				}
				update.executeUpdate();
			}
			List<TaskRecord> claimed = new ArrayList<>();
			try (PreparedStatement query = connection.prepareStatement(
					"""
					SELECT id, task_type, payload, status, available_at, claimed_by, claimed_at,
							attempt_count, created_at, updated_at
					FROM %s
					WHERE id IN (%s)
					AND claimed_by = ?
					ORDER BY available_at, id
					"""
							.formatted(tables.tasks(), placeholders))) {
				for (int i = 0; i < ids.size(); i++) {
					query.setObject(i + 1, ids.get(i));
				}
				query.setObject(ids.size() + 1, workerId);
				try (ResultSet rs = query.executeQuery()) {
					int rowNum = 0;
					while (rs.next()) {
						claimed.add(mapTask(rs, rowNum++));
					}
				}
			}
			return claimed;
		});
	}

	@Override
	public void markInProgress(UUID taskId, UUID workerId, Instant now) {
		jdbcTemplate.update(
				"""
				UPDATE %s
				SET status = 'in_progress', last_progress_at = ?, updated_at = ?
				WHERE id = ? AND claimed_by = ? AND status = 'claimed'
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now),
				Timestamp.from(now),
				taskId,
				workerId);
	}

	@Override
	public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {
		jdbcTemplate.update(
				"""
				UPDATE %s
				SET progress_percent = ?, progress_description = ?, last_progress_at = ?, updated_at = ?
				WHERE id = ? AND claimed_by = ? AND status = 'in_progress'
				"""
						.formatted(tables.tasks()),
				progressPercent,
				truncate(description),
				Timestamp.from(now),
				Timestamp.from(now),
				taskId,
				workerId);
	}

	@Override
	public void markCompleted(UUID taskId, UUID workerId, Instant now) {
		jdbcTemplate.update(
				"""
				UPDATE %s
				SET status = 'completed',
					completed_at = ?,
					progress_percent = 100,
					progress_description = 'finished',
					last_progress_at = ?,
					updated_at = ?
				WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now),
				Timestamp.from(now),
				Timestamp.from(now),
				taskId,
				workerId);
	}

	@Override
	public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
		jdbcTemplate.update(
				"""
				UPDATE %s
				SET status = 'failed', failed_at = ?, updated_at = ?, last_error_message = ?
				WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now),
				Timestamp.from(now),
				truncate(errorMessage),
				taskId,
				workerId);
	}

	@Override
	public void rescheduleAfterFailure(
			UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage) {
		jdbcTemplate.update(
				"""
				UPDATE %s
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
				"""
						.formatted(tables.tasks()),
				Timestamp.from(availableAt),
				Timestamp.from(now),
				truncate(errorMessage),
				taskId,
				workerId);
	}

	@Override
	public void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason) {
		jdbcTemplate.update(
				"""
				UPDATE %s
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
				"""
						.formatted(tables.tasks()),
				Timestamp.from(availableAt),
				Timestamp.from(now),
				truncate(reason),
				taskId,
				workerId);
	}

	@Override
	public int reclaimTimedOut(String taskType, Duration timeout, Instant now) {
		return jdbcTemplate.update(
				"""
				UPDATE %s
				SET status = 'queued',
					claimed_by = NULL,
					claimed_at = NULL,
					progress_percent = NULL,
					progress_description = NULL,
					last_progress_at = NULL,
					updated_at = ?,
					last_error_message = 'Reclaimed after timeout'
				WHERE task_type = ?
				AND status IN ('claimed', 'in_progress')
				AND COALESCE(last_progress_at, claimed_at) < ?
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now),
				taskType,
				Timestamp.from(now.minus(timeout)));
	}

	@Override
	public int markTimedOutFailed(String taskType, Duration timeout, Instant now) {
		return jdbcTemplate.update(
				"""
				UPDATE %s
				SET status = 'failed',
					failed_at = ?,
					updated_at = ?,
					last_error_message = 'Task timed out and reclaim is disabled'
				WHERE task_type = ?
				AND status IN ('claimed', 'in_progress')
				AND COALESCE(last_progress_at, claimed_at) < ?
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now),
				Timestamp.from(now),
				taskType,
				Timestamp.from(now.minus(timeout)));
	}

	@Override
	public int deleteCompletedOlderThan(Duration retention, Instant now) {
		return jdbcTemplate.update(
				"""
				DELETE FROM %s
				WHERE status = 'completed'
				AND completed_at IS NOT NULL
				AND completed_at < ?
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now.minus(retention)));
	}

	@Override
	public int deleteFailedOlderThan(Duration retention, Instant now) {
		return jdbcTemplate.update(
				"""
				DELETE FROM %s
				WHERE status = 'failed'
				AND failed_at IS NOT NULL
				AND failed_at < ?
				"""
						.formatted(tables.tasks()),
				Timestamp.from(now.minus(retention)));
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
