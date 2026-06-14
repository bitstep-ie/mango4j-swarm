package ie.bitstep.mango.swarm.db;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
	private static final String SELECT_QUEUED_TASK_IDS_SQL =
			"""
SELECT id
FROM mango_swarm_tasks
WHERE task_type = ?
AND status = 'queued'
AND available_at <= ?
ORDER BY available_at, id
LIMIT ?
FOR UPDATE SKIP LOCKED
""";
	private static final String CLAIM_TASK_SQL =
			"""
UPDATE mango_swarm_tasks
SET status = 'claimed',
claimed_by = ?,
claimed_at = ?,
attempt_count = attempt_count + 1,
execution_time_ms = NULL,
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
SET status = 'in_progress', execution_time_ms = 0, updated_at = ?
WHERE id = ? AND claimed_by = ? AND status = 'claimed'
""";
	private static final String UPSERT_RUNTIME_SQL =
			"""
INSERT INTO mango_swarm_task_runtime(
task_id, worker_id, execution_state, progress_percent, progress_message, started_at, updated_at, execution_time_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (task_id) DO UPDATE
SET worker_id = EXCLUDED.worker_id,
execution_state = EXCLUDED.execution_state,
progress_percent = EXCLUDED.progress_percent,
progress_message = EXCLUDED.progress_message,
updated_at = EXCLUDED.updated_at,
execution_time_ms = EXCLUDED.execution_time_ms
""";
	private static final String INSERT_RUNTIME_SQL =
			"""
INSERT INTO mango_swarm_task_runtime(
task_id, worker_id, execution_state, progress_percent, progress_message, started_at, updated_at, execution_time_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
execution_time_ms = ?,
updated_at = ?
WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
""";
	private static final String MARK_FAILED_SQL =
			"""
UPDATE mango_swarm_tasks
SET status = 'failed', failed_at = ?, execution_time_ms = ?, updated_at = ?, last_error_message = ?
WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
""";
	private static final String RESCHEDULE_AFTER_FAILURE_SQL =
			"""
UPDATE mango_swarm_tasks
SET status = 'queued',
available_at = ?,
claimed_by = NULL,
claimed_at = NULL,
execution_time_ms = NULL,
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
execution_time_ms = NULL,
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
execution_time_ms = NULL,
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
execution_time_ms = NULL,
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
AND failed_at < ?
ORDER BY failed_at, id
LIMIT ?
)
""";
	private static final int JSON_PARAMETER_INDEX = 3;

	private final JdbcTemplate jdbcTemplate;
	private final ObjectMapper objectMapper;
	private final SchemaQualifiedTables tables;

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
						statement.setObject(4, ts(availableAt));
						statement.executeUpdate();
						return taskId;
					}
				}),
				"queue task");
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
			select.setObject(2, ts(now));
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
			update.setObject(2, ts(now));
			update.setObject(3, ts(now));
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
		Array taskIds = connection.createArrayOf("uuid", ids.toArray());
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
						statement.setObject(1, ts(now));
						statement.setObject(2, taskId);
						statement.setObject(3, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							deleteRuntime(scoped, taskId);
							insertRuntime(scoped, new RuntimeUpdate(taskId, workerId, now, "running", null, null, 0L));
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
					upsertRuntime(
							scoped,
							new RuntimeUpdate(
									taskId,
									workerId,
									now,
									executionState,
									progressPercent,
									message,
									executionTimeMillis(scoped, taskId, now)));
					return 1;
				}),
				"update task runtime");
	}

	@Override
	public void markCompleted(UUID taskId, UUID workerId, Instant now) {
		executeRequired(
				connection -> tables.withSearchPath(connection, scoped -> {
					long executionTimeMillis = executionTimeMillis(scoped, taskId, now);
					try (PreparedStatement statement = scoped.prepareStatement(MARK_COMPLETED_SQL)) {
						statement.setObject(1, ts(now));
						statement.setLong(2, executionTimeMillis);
						statement.setObject(3, ts(now));
						statement.setObject(4, taskId);
						statement.setObject(5, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							upsertRuntime(
									scoped,
									new RuntimeUpdate(
											taskId, workerId, now, "completed", 100, "finished", executionTimeMillis));
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
					long executionTimeMillis = executionTimeMillis(scoped, taskId, now);
					try (PreparedStatement statement = scoped.prepareStatement(MARK_FAILED_SQL)) {
						statement.setObject(1, ts(now));
						statement.setLong(2, executionTimeMillis);
						statement.setObject(3, ts(now));
						statement.setString(4, truncate(errorMessage));
						statement.setObject(5, taskId);
						statement.setObject(6, workerId);
						int updated = statement.executeUpdate();
						if (updated > 0) {
							upsertRuntime(
									scoped,
									new RuntimeUpdate(
											taskId, workerId, now, "failed", null, errorMessage, executionTimeMillis));
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
						statement.setObject(1, ts(availableAt));
						statement.setObject(2, ts(now));
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
						statement.setObject(1, ts(availableAt));
						statement.setObject(2, ts(now));
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
						statement.setObject(1, ts(now));
						statement.setString(2, taskType);
						statement.setObject(3, ts(now.minus(timeout)));
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
						statement.setObject(1, ts(now));
						statement.setObject(2, ts(now));
						statement.setString(3, taskType);
						statement.setObject(4, ts(now.minus(timeout)));
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
						statement.setObject(1, ts(now.minus(retention)));
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
						statement.setObject(1, ts(now.minus(retention)));
						statement.setInt(2, limit);
						return statement.executeUpdate();
					}
				}),
				"delete failed tasks");
	}

	private void upsertRuntime(java.sql.Connection connection, RuntimeUpdate runtime) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(UPSERT_RUNTIME_SQL)) {
			bindRuntimeInsert(statement, runtime);
			statement.executeUpdate();
		}
	}

	private static void bindRuntimeInsert(PreparedStatement statement, RuntimeUpdate runtime) throws SQLException {
		statement.setObject(1, runtime.taskId());
		statement.setObject(2, runtime.workerId());
		statement.setString(3, truncate(runtime.executionState()));
		setNullableInteger(statement, 4, runtime.progressPercent());
		statement.setString(5, truncate(runtime.message()));
		statement.setObject(6, ts(runtime.now()));
		statement.setObject(7, ts(runtime.now()));
		statement.setLong(8, runtime.executionTimeMillis());
	}

	private static void setNullableInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.INTEGER);
		} else {
			statement.setInt(index, value);
		}
	}

	private void insertRuntime(java.sql.Connection connection, RuntimeUpdate runtime) throws SQLException {
		try (PreparedStatement insert = connection.prepareStatement(INSERT_RUNTIME_SQL)) {
			bindRuntimeInsert(insert, runtime);
			insert.executeUpdate();
		}
	}

	private record RuntimeUpdate(
			UUID taskId,
			UUID workerId,
			Instant now,
			String executionState,
			Integer progressPercent,
			String message,
			long executionTimeMillis) {
		private RuntimeUpdate withExecutionTimeMillis(long executionTimeMillis) {
			return new RuntimeUpdate(
					taskId, workerId, now, executionState, progressPercent, message, executionTimeMillis);
		}
	}

	private static void deleteRuntime(java.sql.Connection connection, UUID taskId) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(DELETE_RUNTIME_SQL)) {
			statement.setObject(1, taskId);
			statement.executeUpdate();
		}
	}

	private long executionTimeMillis(java.sql.Connection connection, UUID taskId, Instant now) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(
				"""
				SELECT started_at
				FROM mango_swarm_task_runtime
				WHERE task_id = ?
				""");
		try {
			statement.setObject(1, taskId);
			ResultSet rs = statement.executeQuery();
			try {
				if (rs.next()) {
					return elapsedMillis(getInstant(rs, "started_at"), now);
				}
			} finally {
				rs.close();
			}
		} finally {
			statement.close();
		}
		return 0L;
	}

	private static long elapsedMillis(Instant startedAt, Instant now) {
		return Math.max(0L, Duration.between(startedAt, now).toMillis());
	}

	private void setJson(PreparedStatement statement, JsonNode payload) throws SQLException {
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
			return new TaskRecord(
					rs.getObject("id", UUID.class),
					rs.getString("task_type"),
					objectMapper.readTree(rs.getString("payload")),
					TaskStatus.fromDatabaseValue(rs.getString("status")),
					getInstant(rs, "available_at"),
					rs.getObject("claimed_by", UUID.class),
					getInstant(rs, "claimed_at"),
					rs.getInt("attempt_count"),
					getInstant(rs, "created_at"),
					getInstant(rs, "updated_at"));
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

	private static OffsetDateTime ts(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

	private static Instant getInstant(ResultSet rs, String col) throws SQLException {
		Object value = rs.getObject(col);
		if (value == null) return null;
		if (value instanceof Instant i) return i;
		if (value instanceof OffsetDateTime odt) return odt.toInstant();
		return ((java.util.Date) value).toInstant();
	}

	private <T> T executeRequired(ConnectionCallback<T> callback, String operation) {
		return Objects.requireNonNull(
				jdbcTemplate.execute(callback), () -> "JdbcTemplate.execute returned null for " + operation);
	}
}
