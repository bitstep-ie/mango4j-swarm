package ie.bitstep.mango.swarm.db;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
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
attempt_count = attempt_count + 1,
updated_at = ?
WHERE id = ?
AND status = 'queued'
""";
	private static final String SELECT_CLAIMED_TASKS_SQL =
			"""
SELECT id, task_type, payload, status, available_at, claimed_by, claimed_at,
attempt_count, created_at, updated_at
FROM mango_swarm_tasks
WHERE claimed_by = ?
AND id = ANY (?)
""";
	private static final String MARK_IN_PROGRESS_SQL =
			"""
UPDATE mango_swarm_tasks
SET status = 'in_progress', updated_at = ?
WHERE id = ? AND claimed_by = ? AND status = 'claimed'
""";
	private static final String UPSERT_RUNTIME_SQL =
			"""
INSERT INTO mango_swarm_task_runtime(
task_id, worker_id, execution_state, progress_percent, progress_message, started_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (task_id) DO UPDATE
SET worker_id = EXCLUDED.worker_id,
execution_state = EXCLUDED.execution_state,
progress_percent = EXCLUDED.progress_percent,
progress_message = EXCLUDED.progress_message,
updated_at = EXCLUDED.updated_at
""";
	private static final String INSERT_RUNTIME_H2_SQL =
			"""
INSERT INTO mango_swarm_task_runtime(
task_id, worker_id, execution_state, progress_percent, progress_message, started_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
""";
	private static final String UPDATE_RUNTIME_H2_SQL =
			"""
UPDATE mango_swarm_task_runtime
SET worker_id = ?,
execution_state = ?,
progress_percent = ?,
progress_message = ?,
updated_at = ?
WHERE task_id = ?
""";
	private static final String DELETE_RUNTIME_SQL = """
DELETE FROM mango_swarm_task_runtime
WHERE task_id = ?
""";
	private static final String MARK_COMPLETED_SQL =
			"""
UPDATE mango_swarm_tasks
SET status = 'completed',
completed_at = ?,
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
updated_at = ?,
last_error_message = 'Reclaimed after timeout'
WHERE id IN (
SELECT t.id
FROM mango_swarm_tasks t
LEFT JOIN mango_swarm_task_runtime r ON r.task_id = t.id
WHERE t.task_type = ?
AND t.status IN ('claimed', 'in_progress')
AND COALESCE(r.updated_at, t.claimed_at) < ?
ORDER BY COALESCE(r.updated_at, t.claimed_at), t.id
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
SELECT t.id
FROM mango_swarm_tasks t
LEFT JOIN mango_swarm_task_runtime r ON r.task_id = t.id
WHERE t.task_type = ?
AND t.status IN ('claimed', 'in_progress')
AND COALESCE(r.updated_at, t.claimed_at) < ?
ORDER BY COALESCE(r.updated_at, t.claimed_at), t.id
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
	private static final int JSON_PARAMETER_INDEX = 3;

	private final JdbcTemplate jdbcTemplate;
	private final ObjectMapper objectMapper;
	private final SchemaQualifiedTables tables;
	private volatile boolean h2;
	private volatile boolean h2Initialized;

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
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					UUID taskId = UuidV7.generate();
					try (PreparedStatement statement = scoped.prepareStatement(INSERT_TASK_SQL)) {
						statement.setObject(1, taskId);
						statement.setString(2, taskType);
						setJson(statement, payload);
						statement.setTimestamp(4, Timestamp.from(availableAt));
						statement.executeUpdate();
						return taskId;
					}
				}),
				"queue task");
	}

	@Override
	public synchronized UUID queueInNextSlot(
			String taskType, JsonNode payload, Instant requestedAt, Duration slotSpacing) {
		if (slotSpacing == null || slotSpacing.isZero() || slotSpacing.isNegative()) {
			return queue(taskType, payload, requestedAt);
		}
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					UUID taskId = UuidV7.generate();
					Instant availableAt = reserveSlot(scoped, taskType, taskId, requestedAt, slotSpacing);
					try (PreparedStatement statement = scoped.prepareStatement(INSERT_TASK_SQL)) {
						statement.setObject(1, taskId);
						statement.setString(2, taskType);
						setJson(statement, payload);
						statement.setTimestamp(4, Timestamp.from(availableAt));
						statement.executeUpdate();
						return taskId;
					}
				}),
				"queue task in next slot");
	}

	private Instant reserveSlot(
			java.sql.Connection connection, String taskType, UUID taskId, Instant requestedAt, Duration slotSpacing)
			throws SQLException {
		Instant candidate = Objects.requireNonNull(requestedAt, "requestedAt");
		int attempts = 0;
		Instant reservedAt = null;
		try (PreparedStatement insert = connection.prepareStatement(INSERT_PACER_SQL)) {
			insert.setString(1, taskType);
			insert.setObject(3, taskId);
			while (reservedAt == null) {
				attempts++;
				if (attempts > 50_000) {
					throw new SQLException("Unable to reserve slot after " + attempts + " attempts for taskType="
							+ taskType + " requestedAt=" + requestedAt);
				}
				Instant adjusted = nextCandidate(connection, taskType, candidate, slotSpacing);
				boolean candidateUnchanged = adjusted.equals(candidate);
				if (candidateUnchanged && tryReserveCandidate(insert, candidate)) {
					reservedAt = candidate;
				} else {
					candidate = candidateUnchanged ? candidate.plus(slotSpacing) : adjusted;
				}
			}
		}
		return Objects.requireNonNull(reservedAt, "reserved slot");
	}

	private Instant nextCandidate(
			java.sql.Connection connection, String taskType, Instant candidate, Duration slotSpacing)
			throws SQLException {
		Objects.requireNonNull(candidate, "candidate");
		Instant before = findSlotBefore(connection, taskType, candidate);
		if (before != null && candidate.isBefore(before.plus(slotSpacing))) {
			return before.plus(slotSpacing);
		}
		Instant after = findSlotAfter(connection, taskType, candidate);
		if (after != null && candidate.plus(slotSpacing).isAfter(after)) {
			return after.plus(slotSpacing);
		}
		return candidate;
	}

	private static boolean tryReserveCandidate(PreparedStatement insert, Instant candidate) throws SQLException {
		try {
			insert.setTimestamp(2, Timestamp.from(candidate));
			insert.executeUpdate();
			return true;
		} catch (SQLIntegrityConstraintViolationException ex) {
			return false;
		}
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
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					List<UUID> ids = selectQueuedTaskIds(scoped, taskType, now, limit);
					if (ids.isEmpty()) {
						return List.of();
					}
					claimTasks(scoped, ids, workerId, now);
					return selectClaimedTasks(scoped, ids, workerId);
				}),
				"claim task batch");
	}

	private List<UUID> selectQueuedTaskIds(java.sql.Connection connection, String taskType, Instant now, int limit)
			throws SQLException {
		List<UUID> ids = new ArrayList<>();
		try (PreparedStatement select = connection.prepareStatement(SELECT_QUEUED_TASK_IDS_SQL)) {
			select.setString(1, taskType);
			select.setTimestamp(2, Timestamp.from(now));
			select.setInt(3, limit);
			try (ResultSet rs = select.executeQuery()) {
				while (rs.next()) {
					ids.add(rs.getObject("id", UUID.class));
				}
			}
		}
		return ids;
	}

	private void claimTasks(java.sql.Connection connection, List<UUID> ids, UUID workerId, Instant now)
			throws SQLException {
		try (PreparedStatement update = connection.prepareStatement(CLAIM_TASK_SQL)) {
			update.setObject(1, workerId);
			update.setTimestamp(2, Timestamp.from(now));
			update.setTimestamp(3, Timestamp.from(now));
			for (UUID id : ids) {
				update.setObject(4, id);
				update.addBatch();
			}
			update.executeBatch();
		}
	}

	private List<TaskRecord> selectClaimedTasks(java.sql.Connection connection, List<UUID> ids, UUID workerId)
			throws SQLException {
		Map<UUID, TaskRecord> claimedById = new HashMap<>();
		Array taskIds = connection.createArrayOf(uuidArrayType(), ids.toArray());
		try (PreparedStatement query = connection.prepareStatement(SELECT_CLAIMED_TASKS_SQL)) {
			query.setObject(1, workerId);
			query.setArray(2, taskIds);
			try (ResultSet rs = query.executeQuery()) {
				while (rs.next()) {
					TaskRecord task = Objects.requireNonNull(mapTask(rs), "mapped claimed task");
					claimedById.put(task.id(), task);
				}
			}
		} finally {
			taskIds.free();
		}
		return claimedTasksInSelectionOrder(ids, claimedById);
	}

	private String uuidArrayType() {
		return isH2() ? "UUID" : "uuid";
	}

	private static List<TaskRecord> claimedTasksInSelectionOrder(List<UUID> ids, Map<UUID, TaskRecord> claimedById) {
		List<TaskRecord> claimed = new ArrayList<>();
		for (UUID id : ids) {
			TaskRecord task = claimedById.get(id);
			if (task != null) {
				claimed.add(task);
			}
		}
		return claimed;
	}

	@Override
	public void markInProgress(UUID taskId, UUID workerId, Instant now) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(MARK_IN_PROGRESS_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setObject(2, taskId);
						statement.setObject(3, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							upsertRuntime(scoped, taskId, workerId, now, "running", null, null);
						}
						return updated;
					}
				}),
				"mark task in progress");
	}

	@Override
	public void updateRuntime(
			UUID taskId, UUID workerId, Instant now, String executionState, Integer progressPercent, String message) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					upsertRuntime(scoped, taskId, workerId, now, executionState, progressPercent, message);
					return 1;
				}),
				"update task runtime");
	}

	@Override
	public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					upsertRuntime(scoped, taskId, workerId, now, "running", progressPercent, description);
					return 1;
				}),
				"record task progress");
	}

	@Override
	public void markCompleted(UUID taskId, UUID workerId, Instant now) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(MARK_COMPLETED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setObject(3, taskId);
						statement.setObject(4, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							upsertRuntime(scoped, taskId, workerId, now, "completed", 100, "finished");
						}
						return updated;
					}
				}),
				"mark task completed");
	}

	@Override
	public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(MARK_FAILED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setString(3, truncate(errorMessage));
						statement.setObject(4, taskId);
						statement.setObject(5, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							upsertRuntime(scoped, taskId, workerId, now, "failed", null, errorMessage);
						}
						return updated;
					}
				}),
				"mark task failed");
	}

	@Override
	public void rescheduleAfterFailure(
			UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(RESCHEDULE_AFTER_FAILURE_SQL)) {
						statement.setTimestamp(1, Timestamp.from(availableAt));
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setString(3, truncate(errorMessage));
						statement.setObject(4, taskId);
						statement.setObject(5, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							deleteRuntime(scoped, taskId);
						}
						return updated;
					}
				}),
				"reschedule failed task");
	}

	@Override
	public void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(REQUEUE_CLAIMED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(availableAt));
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setString(3, truncate(reason));
						statement.setObject(4, taskId);
						statement.setObject(5, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							deleteRuntime(scoped, taskId);
						}
						return updated;
					}
				}),
				"requeue claimed task");
	}

	@Override
	public int reclaimTimedOut(String taskType, Duration timeout, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(RECLAIM_TIMED_OUT_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setString(2, taskType);
						statement.setTimestamp(3, Timestamp.from(now.minus(timeout)));
						statement.setInt(4, limit);
						return statement.executeUpdate();
					}
				}),
				"reclaim timed out tasks");
	}

	@Override
	public int markTimedOutFailed(String taskType, Duration timeout, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(MARK_TIMED_OUT_FAILED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now));
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setString(3, taskType);
						statement.setTimestamp(4, Timestamp.from(now.minus(timeout)));
						statement.setInt(5, limit);
						return statement.executeUpdate();
					}
				}),
				"mark timed out tasks failed");
	}

	@Override
	public int deleteCompletedOlderThan(Duration retention, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_COMPLETED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}),
				"delete completed tasks");
	}

	@Override
	public int deleteFailedOlderThan(Duration retention, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_FAILED_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}),
				"delete failed tasks");
	}

	@Override
	public int deleteTaskPacersOlderThan(Duration retention, Instant now, int limit) {
		if (limit < 1) {
			return 0;
		}
		return executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_PACERS_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}),
				"delete task pacers");
	}

	private void upsertRuntime(
			java.sql.Connection connection,
			UUID taskId,
			UUID workerId,
			Instant now,
			String executionState,
			Integer progressPercent,
			String message)
			throws SQLException {
		if (isH2()) {
			try (PreparedStatement update = connection.prepareStatement(UPDATE_RUNTIME_H2_SQL)) {
				update.setObject(1, workerId);
				update.setString(2, truncate(executionState));
				setNullableInteger(update, 3, progressPercent);
				update.setString(4, truncate(message));
				update.setTimestamp(5, Timestamp.from(now));
				update.setObject(6, taskId);
				if (update.executeUpdate() > 0) {
					return;
				}
			}
			try (PreparedStatement insert = connection.prepareStatement(INSERT_RUNTIME_H2_SQL)) {
				bindRuntimeInsert(insert, taskId, workerId, now, executionState, progressPercent, message);
				insert.executeUpdate();
			}
			return;
		}
		try (PreparedStatement statement = connection.prepareStatement(UPSERT_RUNTIME_SQL)) {
			bindRuntimeInsert(statement, taskId, workerId, now, executionState, progressPercent, message);
			statement.executeUpdate();
		}
	}

	private static void bindRuntimeInsert(
			PreparedStatement statement,
			UUID taskId,
			UUID workerId,
			Instant now,
			String executionState,
			Integer progressPercent,
			String message)
			throws SQLException {
		statement.setObject(1, taskId);
		statement.setObject(2, workerId);
		statement.setString(3, truncate(executionState));
		setNullableInteger(statement, 4, progressPercent);
		statement.setString(5, truncate(message));
		statement.setTimestamp(6, Timestamp.from(now));
		statement.setTimestamp(7, Timestamp.from(now));
	}

	private static void setNullableInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.INTEGER);
		} else {
			statement.setInt(index, value);
		}
	}

	private static void deleteRuntime(java.sql.Connection connection, UUID taskId) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(DELETE_RUNTIME_SQL)) {
			statement.setObject(1, taskId);
			statement.executeUpdate();
		}
	}

	private void setJson(PreparedStatement statement, JsonNode payload) throws SQLException {
		if (isH2()) {
			statement.setString(JSON_PARAMETER_INDEX, json(payload));
			return;
		}
		PGobject object = new PGobject();
		object.setType("jsonb");
		object.setValue(json(payload));
		statement.setObject(JSON_PARAMETER_INDEX, object, Types.OTHER);
	}

	private String json(JsonNode payload) throws SQLException {
		try {
			return objectMapper.writeValueAsString(payload);
		} catch (JsonProcessingException ex) {
			throw new SQLException("Cannot serialize task payload", ex);
		}
	}

	TaskRecord mapTask(ResultSet rs) throws SQLException {
		try {
			Timestamp claimedAt = rs.getTimestamp("claimed_at");
			return new TaskRecord(
					rs.getObject("id", UUID.class),
					rs.getString("task_type"),
					objectMapper.readTree(rs.getString("payload")),
					TaskStatus.fromDatabaseValue(rs.getString("status")),
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
		if (!h2Initialized) {
			synchronized (this) {
				if (!h2Initialized) {
					h2 = detectH2();
					h2Initialized = true;
				}
			}
		}
		return h2;
	}

	private boolean detectH2() {
		return executeRequired(
				connection -> {
					String productName = connection.getMetaData().getDatabaseProductName();
					return productName != null && productName.toLowerCase().contains("h2");
				},
				"detect database product");
	}

	private <T> T executeRequired(ConnectionCallback<T> callback, String operation) {
		return Objects.requireNonNull(
				jdbcTemplate.execute(callback), () -> "JdbcTemplate.execute returned null for " + operation);
	}
}
