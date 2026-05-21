package ie.bitstep.mango.swarm.db;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import ie.bitstep.mango.swarm.PostgresTestSupport;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskRepositoryTest extends PostgresTestSupport {

    @Test
    void claimsTasksInBatches() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        for (int i = 0; i < 5; i++) {
            taskRepository.queue("email", JsonNodeFactory.instance.objectNode().put("i", i), now);
        }

        List<TaskRecord> claimed = taskRepository.claimBatch("email", UUID.randomUUID(), now, 3);

        assertThat(claimed).hasSize(3);
        Integer queued = jdbcTemplate.queryForObject(
                "select count(*) from mango_swarm_tasks where status = 'queued'",
                Integer.class);
        assertThat(queued).isEqualTo(2);
    }

    @Test
    void queueInNextSlotPushesRequestedTimePastLatestSlot() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        UUID first = taskRepository.queueInNextSlot(
                "email",
                JsonNodeFactory.instance.objectNode().put("i", 1),
                now,
                java.time.Duration.ofMillis(10));
        UUID second = taskRepository.queueInNextSlot(
                "email",
                JsonNodeFactory.instance.objectNode().put("i", 2),
                now,
                java.time.Duration.ofMillis(10));
        UUID third = taskRepository.queueInNextSlot(
                "email",
                JsonNodeFactory.instance.objectNode().put("i", 3),
                now.plusSeconds(1),
                java.time.Duration.ofMillis(10));

        var rows = jdbcTemplate.queryForList("""
                select id, available_at
                from mango_swarm_tasks
                order by available_at
                """);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).get("id")).isEqualTo(first);
        assertThat(((java.sql.Timestamp) rows.get(0).get("available_at")).toInstant()).isEqualTo(now);
        assertThat(rows.get(1).get("id")).isEqualTo(second);
        assertThat(((java.sql.Timestamp) rows.get(1).get("available_at")).toInstant())
                .isEqualTo(now.plusMillis(10));
        assertThat(rows.get(2).get("id")).isEqualTo(third);
        assertThat(((java.sql.Timestamp) rows.get(2).get("available_at")).toInstant())
                .isEqualTo(now.plusSeconds(1));
    }

    @Test
    void farFutureTaskDoesNotBlockEarlierScheduleSlots() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        Instant future = now.plusSeconds(3600);
        taskRepository.queueInNextSlot(
                "email",
                JsonNodeFactory.instance.objectNode().put("i", "future"),
                future,
                java.time.Duration.ofMillis(10));

        UUID immediate = taskRepository.queueInNextSlot(
                "email",
                JsonNodeFactory.instance.objectNode().put("i", "now"),
                now,
                java.time.Duration.ofMillis(10));
        UUID beforeFuture = taskRepository.queueInNextSlot(
                "email",
                JsonNodeFactory.instance.objectNode().put("i", "before-future"),
                future.minusSeconds(1),
                java.time.Duration.ofMillis(10));

        var rows = jdbcTemplate.queryForList("""
                select id, available_at
                from mango_swarm_tasks
                where id in (?, ?)
                order by available_at
                """, immediate, beforeFuture);
        assertThat(((java.sql.Timestamp) rows.get(0).get("available_at")).toInstant()).isEqualTo(now);
        assertThat(((java.sql.Timestamp) rows.get(1).get("available_at")).toInstant()).isEqualTo(future.minusSeconds(1));
    }

    @Test
    void concurrentClaimsDoNotClaimSameTaskTwice() throws Exception {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        for (int i = 0; i < 20; i++) {
            taskRepository.queue("email", JsonNodeFactory.instance.objectNode().put("i", i), now);
        }
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        var first = executor.submit(() -> {
            start.await();
            return taskRepository.claimBatch("email", UUID.randomUUID(), now, 12);
        });
        var second = executor.submit(() -> {
            start.await();
            return taskRepository.claimBatch("email", UUID.randomUUID(), now, 12);
        });

        start.countDown();
        List<TaskRecord> all = new java.util.ArrayList<>();
        all.addAll(first.get(10, TimeUnit.SECONDS));
        all.addAll(second.get(10, TimeUnit.SECONDS));
        executor.shutdownNow();

        assertThat(all).extracting(TaskRecord::id).doesNotHaveDuplicates();
        assertThat(all).hasSize(20);
    }

    @Test
    void reclaimsOnlyWhenRequestedByCaller() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
        taskRepository.claimBatch("email", UUID.randomUUID(), now.minusSeconds(60), 1);

        int reclaimed = taskRepository.reclaimTimedOut("email", java.time.Duration.ofSeconds(30), now);

        assertThat(reclaimed).isEqualTo(1);
        String status = jdbcTemplate.queryForObject("select status from mango_swarm_tasks", String.class);
        assertThat(status).isEqualTo("queued");
    }

    @Test
    void progressUpdatesLivenessForTimeoutRecovery() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        UUID workerId = UUID.randomUUID();
        UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
        taskRepository.claimBatch("email", workerId, now.minusSeconds(60), 1);
        taskRepository.markInProgress(taskId, workerId, now.minusSeconds(60));

        taskRepository.recordProgress(taskId, workerId, now.minusSeconds(5), 50, "sending");
        int reclaimed = taskRepository.reclaimTimedOut("email", java.time.Duration.ofSeconds(30), now);

        assertThat(reclaimed).isZero();
        var row = jdbcTemplate.queryForMap("""
                select status, progress_percent, progress_description, last_progress_at
                from mango_swarm_tasks
                where id = ?
                """, taskId);
        assertThat(row.get("status")).isEqualTo("in_progress");
        assertThat(row.get("progress_percent")).isEqualTo(50);
        assertThat(row.get("progress_description")).isEqualTo("sending");
        assertThat(((java.sql.Timestamp) row.get("last_progress_at")).toInstant()).isEqualTo(now.minusSeconds(5));
    }

    @Test
    void completionRecordsFinalProgress() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        UUID workerId = UUID.randomUUID();
        UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
        taskRepository.claimBatch("email", workerId, now, 1);
        taskRepository.markInProgress(taskId, workerId, now);

        taskRepository.markCompleted(taskId, workerId, now.plusSeconds(5));

        var row = jdbcTemplate.queryForMap("""
                select status, progress_percent, progress_description, last_progress_at
                from mango_swarm_tasks
                where id = ?
                """, taskId);
        assertThat(row.get("status")).isEqualTo("completed");
        assertThat(row.get("progress_percent")).isEqualTo(100);
        assertThat(row.get("progress_description")).isEqualTo("finished");
        assertThat(((java.sql.Timestamp) row.get("last_progress_at")).toInstant()).isEqualTo(now.plusSeconds(5));
    }

    @Test
    void reschedulesFailedAttemptUsingSameTaskRow() {
        Instant now = Instant.parse("2026-05-20T10:00:00Z");
        Instant retryAt = now.plusSeconds(10);
        UUID workerId = UUID.randomUUID();
        UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
        taskRepository.claimBatch("email", workerId, now, 1);
        taskRepository.markInProgress(taskId, workerId, now);

        taskRepository.rescheduleAfterFailure(taskId, workerId, now, retryAt, "temporary failure");

        var row = jdbcTemplate.queryForMap("""
                select status, available_at, claimed_by, claimed_at, failed_at, last_error_message
                from mango_swarm_tasks
                where id = ?
                """, taskId);
        assertThat(row.get("status")).isEqualTo("queued");
        assertThat(((java.sql.Timestamp) row.get("available_at")).toInstant()).isEqualTo(retryAt);
        assertThat(row.get("claimed_by")).isNull();
        assertThat(row.get("claimed_at")).isNull();
        assertThat(row.get("failed_at")).isNull();
        assertThat(row.get("last_error_message")).isEqualTo("temporary failure");
    }
}
