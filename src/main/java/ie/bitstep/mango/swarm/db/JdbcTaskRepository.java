package ie.bitstep.mango.swarm.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.bitstep.mango.swarm.TaskStatus;
import ie.bitstep.mango.swarm.UuidV7;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * JDBC/PostgreSQL implementation of {@link TaskRepository}.
 * <p>
 * Uses SQL row locking and task-state updates to support safe multi-worker claiming,
 * progress tracking, retry rescheduling, timeout recovery, and retention cleanup.
 */
public class JdbcTaskRepository implements TaskRepository {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final SchemaQualifiedTables tables;
    private final RowMapper<TaskRecord> rowMapper = this::mapTask;

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
            try (PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO %s(id, task_type, payload, status, available_at)
                    VALUES (?, ?, ?, 'queued', ?)
                    """.formatted(tables.tasks()))) {
                statement.setObject(1, taskId);
                statement.setString(2, taskType);
                statement.setObject(3, jsonb(payload), Types.OTHER);
                statement.setTimestamp(4, Timestamp.from(availableAt));
                statement.executeUpdate();
                return taskId;
            }
        });
    }

    @Override
    public UUID queueInNextSlot(String taskType, JsonNode payload, Instant requestedAt, Duration slotSpacing) {
        if (slotSpacing == null || slotSpacing.isZero() || slotSpacing.isNegative()) {
            return queue(taskType, payload, requestedAt);
        }
        return jdbcTemplate.execute((ConnectionCallback<UUID>) connection -> {
            boolean localTransaction = connection.getAutoCommit();
            if (localTransaction) {
                connection.setAutoCommit(false);
            }
            try {
                try (PreparedStatement lock = connection.prepareStatement("SELECT pg_advisory_lock(hashtext(?)::bigint)")) {
                    lock.setString(1, taskType);
                    lock.execute();
                }
                UUID taskId = UuidV7.generate();
                Instant availableAt = reserveSlot(connection, taskType, taskId, requestedAt, slotSpacing);
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO %s(id, task_type, payload, status, available_at)
                        VALUES (?, ?, ?, 'queued', ?)
                        """.formatted(tables.tasks()))) {
                    statement.setObject(1, taskId);
                    statement.setString(2, taskType);
                    statement.setObject(3, jsonb(payload), Types.OTHER);
                    statement.setTimestamp(4, Timestamp.from(availableAt));
                    statement.executeUpdate();
                    if (localTransaction) {
                        connection.commit();
                    }
                    return taskId;
                }
            } catch (Exception ex) {
                if (localTransaction) {
                    connection.rollback();
                }
                throw ex;
            } finally {
                try (PreparedStatement unlock = connection.prepareStatement("SELECT pg_advisory_unlock(hashtext(?)::bigint)")) {
                    unlock.setString(1, taskType);
                    unlock.execute();
                }
                if (localTransaction) {
                    connection.setAutoCommit(true);
                }
            }
        });
    }

    private Instant reserveSlot(
            java.sql.Connection connection,
            String taskType,
            UUID taskId,
            Instant requestedAt,
            Duration slotSpacing) throws SQLException {
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
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO %s(task_type, slot_at, task_id)
                    VALUES (?, ?, ?)
                    """.formatted(tables.taskPacers()))) {
                insert.setString(1, taskType);
                insert.setTimestamp(2, Timestamp.from(candidate));
                insert.setObject(3, taskId);
                insert.executeUpdate();
            }
            return candidate;
        }
    }

    private Instant findSlot(java.sql.Connection connection, String taskType, Instant candidate, boolean before)
            throws SQLException {
        String comparison = before ? "<=" : ">";
        String ordering = before ? "DESC" : "ASC";
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT slot_at
                FROM %s
                WHERE task_type = ?
                  AND slot_at %s ?
                ORDER BY slot_at %s
                LIMIT 1
                """.formatted(tables.taskPacers(), comparison, ordering))) {
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
        if (limit <= 0) {
            return List.of();
        }
        return jdbcTemplate.query("""
                UPDATE %s t
                SET status = 'claimed',
                    claimed_by = ?,
                    claimed_at = ?,
                    progress_percent = NULL,
                    progress_description = NULL,
                    last_progress_at = NULL,
                    attempt_count = attempt_count + 1,
                    updated_at = ?
                WHERE t.id IN (
                    SELECT id
                    FROM %s
                    WHERE task_type = ?
                      AND status = 'queued'
                      AND available_at <= ?
                    ORDER BY available_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT ?
                )
                RETURNING id, task_type, payload, status, available_at, claimed_by, claimed_at,
                          attempt_count, created_at, updated_at
                """.formatted(tables.tasks(), tables.tasks()),
                rowMapper, workerId, Timestamp.from(now), Timestamp.from(now), taskType, Timestamp.from(now), limit);
    }

    @Override
    public void markInProgress(UUID taskId, UUID workerId, Instant now) {
        jdbcTemplate.update("""
                UPDATE %s
                SET status = 'in_progress', last_progress_at = ?, updated_at = ?
                WHERE id = ? AND claimed_by = ? AND status = 'claimed'
                """.formatted(tables.tasks()), Timestamp.from(now), Timestamp.from(now), taskId, workerId);
    }

    @Override
    public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {
        jdbcTemplate.update("""
                UPDATE %s
                SET progress_percent = ?, progress_description = ?, last_progress_at = ?, updated_at = ?
                WHERE id = ? AND claimed_by = ? AND status = 'in_progress'
                """.formatted(tables.tasks()),
                progressPercent,
                truncate(description),
                Timestamp.from(now),
                Timestamp.from(now),
                taskId,
                workerId);
    }

    @Override
    public void markCompleted(UUID taskId, UUID workerId, Instant now) {
        jdbcTemplate.update("""
                UPDATE %s
                SET status = 'completed',
                    completed_at = ?,
                    progress_percent = 100,
                    progress_description = 'finished',
                    last_progress_at = ?,
                    updated_at = ?
                WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
                """.formatted(tables.tasks()),
                Timestamp.from(now),
                Timestamp.from(now),
                Timestamp.from(now),
                taskId,
                workerId);
    }

    @Override
    public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
        jdbcTemplate.update("""
                UPDATE %s
                SET status = 'failed', failed_at = ?, updated_at = ?, last_error_message = ?
                WHERE id = ? AND claimed_by = ? AND status IN ('claimed', 'in_progress')
                """.formatted(tables.tasks()), Timestamp.from(now), Timestamp.from(now), truncate(errorMessage), taskId, workerId);
    }

    @Override
    public void rescheduleAfterFailure(UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage) {
        jdbcTemplate.update("""
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
                """.formatted(tables.tasks()),
                Timestamp.from(availableAt),
                Timestamp.from(now),
                truncate(errorMessage),
                taskId,
                workerId);
    }

    @Override
    public void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason) {
        jdbcTemplate.update("""
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
                """.formatted(tables.tasks()),
                Timestamp.from(availableAt),
                Timestamp.from(now),
                truncate(reason),
                taskId,
                workerId);
    }

    @Override
    public int reclaimTimedOut(String taskType, Duration timeout, Instant now) {
        return jdbcTemplate.update("""
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
                """.formatted(tables.tasks()), Timestamp.from(now), taskType, Timestamp.from(now.minus(timeout)));
    }

    @Override
    public int markTimedOutFailed(String taskType, Duration timeout, Instant now) {
        return jdbcTemplate.update("""
                UPDATE %s
                SET status = 'failed',
                    failed_at = ?,
                    updated_at = ?,
                    last_error_message = 'Task timed out and reclaim is disabled'
                WHERE task_type = ?
                  AND status IN ('claimed', 'in_progress')
                  AND COALESCE(last_progress_at, claimed_at) < ?
                """.formatted(tables.tasks()), Timestamp.from(now), Timestamp.from(now), taskType, Timestamp.from(now.minus(timeout)));
    }

    @Override
    public int deleteCompletedOlderThan(Duration retention, Instant now) {
        return jdbcTemplate.update("""
                DELETE FROM %s
                WHERE status = 'completed'
                  AND completed_at IS NOT NULL
                  AND completed_at < ?
                """.formatted(tables.tasks()), Timestamp.from(now.minus(retention)));
    }

    @Override
    public int deleteFailedOlderThan(Duration retention, Instant now) {
        return jdbcTemplate.update("""
                DELETE FROM %s
                WHERE status = 'failed'
                  AND failed_at IS NOT NULL
                  AND failed_at < ?
                """.formatted(tables.tasks()), Timestamp.from(now.minus(retention)));
    }

    private PGobject jsonb(JsonNode payload) throws SQLException {
        PGobject object = new PGobject();
        object.setType("jsonb");
        try {
            object.setValue(objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException ex) {
            throw new SQLException("Cannot serialize task payload", ex);
        }
        return object;
    }

    private TaskRecord mapTask(ResultSet rs, int rowNum) throws SQLException {
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
        return message.length() <= 4000 ? message : message.substring(0, 4000);
    }
}
