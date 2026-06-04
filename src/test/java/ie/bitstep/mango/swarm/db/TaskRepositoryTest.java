package ie.bitstep.mango.swarm.db;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ie.bitstep.mango.swarm.H2TestSupport;
import ie.bitstep.mango.swarm.TaskStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskRepositoryTest extends H2TestSupport {

	@Test
	void claimsTasksInBatches() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		for (int i = 0; i < 5; i++) {
			taskRepository.queue("email", JsonNodeFactory.instance.objectNode().put("i", i), now);
		}

		List<TaskRecord> claimed = taskRepository.claimBatch("email", UUID.randomUUID(), now, 3);

		assertThat(claimed).hasSize(3);
		Integer queued = jdbcTemplate.queryForObject(
				"select count(*) from mango_swarm_tasks where status = 'queued'", Integer.class);
		assertThat(queued).isEqualTo(2);
	}

	@Test
	void zeroClaimLimitDoesNotClaimTasks() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);

		List<TaskRecord> claimed = taskRepository.claimBatch("email", UUID.randomUUID(), now, 0);

		assertThat(claimed).isEmpty();
		assertThat(jdbcTemplate.queryForObject("select status from mango_swarm_tasks", String.class))
				.isEqualTo(TaskStatus.QUEUED.databaseValue());
	}

	@Test
	void claimBatchReturnsEmptyWhenNoTasksAreQueued() {
		assertThat(taskRepository.claimBatch("email", UUID.randomUUID(), Instant.parse("2026-05-20T10:00:00Z"), 10))
				.isEmpty();
	}

	@Test
	void mapsUnclaimedTaskRowsWithNullClaimTimestamp() throws Exception {
		Instant availableAt = Instant.parse("2026-05-20T10:00:00Z");
		Instant createdAt = Instant.parse("2026-05-20T09:59:59Z");
		Instant updatedAt = Instant.parse("2026-05-20T10:00:01Z");
		UUID taskId = UUID.randomUUID();
		UUID workerId = UUID.randomUUID();
		Map<String, Object> row = new HashMap<>();
		row.put("id", taskId);
		row.put("task_type", "email");
		row.put("payload", "{\"subject\":\"hello\"}");
		row.put("status", TaskStatus.QUEUED.databaseValue());
		row.put("available_at", Timestamp.from(availableAt));
		row.put("claimed_by", workerId);
		row.put("claimed_at", null);
		row.put("attempt_count", 0);
		row.put("created_at", Timestamp.from(createdAt));
		row.put("updated_at", Timestamp.from(updatedAt));

		TaskRecord task = ((JdbcTaskRepository) taskRepository).mapTask(resultSet(row));

		assertThat(task.id()).isEqualTo(taskId);
		assertThat(task.taskType()).isEqualTo("email");
		assertThat(task.status()).isEqualTo(TaskStatus.QUEUED);
		assertThat(task.availableAt()).isEqualTo(availableAt);
		assertThat(task.claimedBy()).isEqualTo(workerId);
		assertThat(task.claimedAt()).isNull();
		assertThat(task.attemptCount()).isZero();
		assertThat(task.createdAt()).isEqualTo(createdAt);
		assertThat(task.updatedAt()).isEqualTo(updatedAt);
		assertThat(task.payload().get("subject").asText()).isEqualTo("hello");
	}

	@Test
	void mapsClaimedTaskRowsWithClaimTimestamp() throws Exception {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		Map<String, Object> row = new HashMap<>();
		row.put("id", UUID.randomUUID());
		row.put("task_type", "email");
		row.put("payload", "{}");
		row.put("status", TaskStatus.CLAIMED.databaseValue());
		row.put("available_at", Timestamp.from(now.minusSeconds(1)));
		row.put("claimed_by", UUID.randomUUID());
		row.put("claimed_at", Timestamp.from(now));
		row.put("attempt_count", 1);
		row.put("created_at", Timestamp.from(now.minusSeconds(2)));
		row.put("updated_at", Timestamp.from(now));

		TaskRecord task = ((JdbcTaskRepository) taskRepository).mapTask(resultSet(row));

		assertThat(task.claimedAt()).isEqualTo(now);
	}

	@Test
	void invalidJsonPayloadRowsFailMapping() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		Map<String, Object> row = new HashMap<>();
		row.put("id", UUID.randomUUID());
		row.put("task_type", "email");
		row.put("payload", "{");
		row.put("status", TaskStatus.QUEUED.databaseValue());
		row.put("available_at", Timestamp.from(now));
		row.put("claimed_by", null);
		row.put("claimed_at", null);
		row.put("attempt_count", 0);
		row.put("created_at", Timestamp.from(now));
		row.put("updated_at", Timestamp.from(now));

		assertThatThrownBy(() -> ((JdbcTaskRepository) taskRepository).mapTask(resultSet(row)))
				.isInstanceOf(java.sql.SQLException.class)
				.hasMessage("Cannot parse task payload");
	}

	@Test
	void queueStoresRequestedEligibilityTimeWithoutSmoothing() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID first = taskRepository.queue(
				"email", JsonNodeFactory.instance.objectNode().put("i", 1), now);
		UUID second = taskRepository.queue(
				"email", JsonNodeFactory.instance.objectNode().put("i", 2), now);
		UUID third = taskRepository.queue(
				"email", JsonNodeFactory.instance.objectNode().put("i", 3), now.plusSeconds(1));

		var rows = jdbcTemplate.queryForList(
				"""
				select id, available_at
				from mango_swarm_tasks
				order by available_at, id
				""");
		assertThat(rows).hasSize(3);
		assertThat(rows).extracting(row -> row.get("id")).containsExactlyInAnyOrder(first, second, third);
		assertThat(((java.sql.Timestamp) rows.get(0).get("available_at")).toInstant())
				.isEqualTo(now);
		assertThat(((java.sql.Timestamp) rows.get(1).get("available_at")).toInstant())
				.isEqualTo(now);
		assertThat(((java.sql.Timestamp) rows.get(2).get("available_at")).toInstant())
				.isEqualTo(now.plusSeconds(1));
	}

	@Test
	void queueParticipatesInExistingTransaction() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		TransactionTemplate transactionTemplate =
				new TransactionTemplate(new DataSourceTransactionManager(jdbcTemplate.getDataSource()));

		UUID taskId = transactionTemplate.execute(status -> taskRepository.queue(
				"email", JsonNodeFactory.instance.objectNode().put("i", "transactional"), now));

		assertThat(taskId).isNotNull();
		assertThat(jdbcTemplate.queryForObject("select count(*) from mango_swarm_tasks", Integer.class))
				.isEqualTo(1);
	}

	@Test
	@Disabled("Requires PostgreSQL row-lock SKIP LOCKED semantics that H2 cannot model")
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

		int reclaimed = taskRepository.reclaimTimedOut("email", java.time.Duration.ofSeconds(30), now, 10);

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
		int reclaimed = taskRepository.reclaimTimedOut("email", java.time.Duration.ofSeconds(30), now, 10);

		assertThat(reclaimed).isZero();
		var row = jdbcTemplate.queryForMap(
				"""
					select t.status, r.execution_state, r.progress_percent, r.progress_message, r.updated_at, r.execution_time_ms
					from mango_swarm_tasks t
					join mango_swarm_task_runtime r on r.task_id = t.id
					where t.id = ?
				""",
				taskId);
		assertThat(row)
				.containsEntry("status", "in_progress")
				.containsEntry("execution_state", "running")
				.containsEntry("progress_percent", 50)
				.containsEntry("progress_message", "sending")
				.containsEntry("execution_time_ms", 55_000L);
		assertThat(((java.sql.Timestamp) row.get("updated_at")).toInstant()).isEqualTo(now.minusSeconds(5));
	}

	@Test
	void markInProgressCreatesRuntimeRow() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);

		taskRepository.markInProgress(taskId, workerId, now.plusSeconds(1));

		var row = jdbcTemplate.queryForMap(
				"""
					select worker_id, execution_state, progress_percent, progress_message, started_at, updated_at, execution_time_ms
					from mango_swarm_task_runtime
					where task_id = ?
					""",
				taskId);
		assertThat(row)
				.containsEntry("worker_id", workerId)
				.containsEntry("execution_state", "running")
				.containsEntry("progress_percent", null)
				.containsEntry("progress_message", null)
				.containsEntry("execution_time_ms", 0L);
		assertThat(((java.sql.Timestamp) row.get("started_at")).toInstant()).isEqualTo(now.plusSeconds(1));
		assertThat(((java.sql.Timestamp) row.get("updated_at")).toInstant()).isEqualTo(now.plusSeconds(1));
		assertThat(jdbcTemplate.queryForObject(
						"select execution_time_ms from mango_swarm_tasks where id = ?", Long.class, taskId))
				.isZero();
	}

	@Test
	void markInProgressDoesNothingForDifferentWorker() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);

		taskRepository.markInProgress(taskId, UUID.randomUUID(), now.plusSeconds(1));

		assertThat(jdbcTemplate.queryForObject(
						"select status from mango_swarm_tasks where id = ?", String.class, taskId))
				.isEqualTo(TaskStatus.CLAIMED.databaseValue());
		assertThat(jdbcTemplate.queryForObject(
						"select count(*) from mango_swarm_task_runtime where task_id = ?", Integer.class, taskId))
				.isZero();
	}

	@Test
	void updateRuntimeUpsertsMutableExecutionState() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.updateRuntime(taskId, workerId, now.plusSeconds(1), "Downloading file", 30, "downloading");

		taskRepository.updateRuntime(taskId, workerId, now.plusSeconds(2), "Calling partner API", 75, "calling");

		var row = jdbcTemplate.queryForMap(
				"""
					select worker_id, execution_state, progress_percent, progress_message, started_at, updated_at, execution_time_ms
					from mango_swarm_task_runtime
					where task_id = ?
					""",
				taskId);
		assertThat(row)
				.containsEntry("worker_id", workerId)
				.containsEntry("execution_state", "Calling partner API")
				.containsEntry("progress_percent", 75)
				.containsEntry("progress_message", "calling")
				.containsEntry("execution_time_ms", 1_000L);
		assertThat(((java.sql.Timestamp) row.get("started_at")).toInstant()).isEqualTo(now.plusSeconds(1));
		assertThat(((java.sql.Timestamp) row.get("updated_at")).toInstant()).isEqualTo(now.plusSeconds(2));
	}

	@Test
	void completionRecordsFinalProgress() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.markInProgress(taskId, workerId, now);

		taskRepository.markCompleted(taskId, workerId, now.plusSeconds(5));

		var row = jdbcTemplate.queryForMap(
				"""
					select t.status, t.execution_time_ms task_execution_time_ms,
						r.execution_state, r.progress_percent, r.progress_message, r.updated_at,
						r.execution_time_ms runtime_execution_time_ms
					from mango_swarm_tasks t
					join mango_swarm_task_runtime r on r.task_id = t.id
					where t.id = ?
				""",
				taskId);
		assertThat(row)
				.containsEntry("status", "completed")
				.containsEntry("task_execution_time_ms", 5_000L)
				.containsEntry("execution_state", "completed")
				.containsEntry("progress_percent", 100)
				.containsEntry("progress_message", "finished")
				.containsEntry("runtime_execution_time_ms", 5_000L);
		assertThat(((java.sql.Timestamp) row.get("updated_at")).toInstant()).isEqualTo(now.plusSeconds(5));
	}

	@Test
	void completionDoesNothingForDifferentWorker() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.markInProgress(taskId, workerId, now);

		taskRepository.markCompleted(taskId, UUID.randomUUID(), now.plusSeconds(5));

		var row = jdbcTemplate.queryForMap(
				"""
				select status, completed_at, execution_time_ms
				from mango_swarm_tasks
				where id = ?
				""",
				taskId);
		assertThat(row)
				.containsEntry("status", TaskStatus.IN_PROGRESS.databaseValue())
				.containsEntry("completed_at", null)
				.containsEntry("execution_time_ms", 0L);
		assertThat(jdbcTemplate.queryForObject(
						"select execution_state from mango_swarm_task_runtime where task_id = ?", String.class, taskId))
				.isEqualTo("running");
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

		var row = jdbcTemplate.queryForMap(
				"""
					select status, available_at, claimed_by, claimed_at, failed_at, execution_time_ms, last_error_message
					from mango_swarm_tasks
					where id = ?
					""",
				taskId);
		assertThat(row).containsEntry("status", "queued");
		assertThat(((java.sql.Timestamp) row.get("available_at")).toInstant()).isEqualTo(retryAt);
		assertThat(row.get("claimed_by")).isNull();
		assertThat(row.get("claimed_at")).isNull();
		assertThat(row.get("failed_at")).isNull();
		assertThat(row.get("execution_time_ms")).isNull();
		assertThat(row).containsEntry("last_error_message", "temporary failure");
		assertThat(jdbcTemplate.queryForObject(
						"select count(*) from mango_swarm_task_runtime where task_id = ?", Integer.class, taskId))
				.isZero();
	}

	@Test
	void rescheduleDoesNothingForDifferentWorker() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.markInProgress(taskId, workerId, now);

		taskRepository.rescheduleAfterFailure(
				taskId, UUID.randomUUID(), now.plusSeconds(1), now.plusSeconds(10), "wrong worker");

		assertThat(jdbcTemplate.queryForObject(
						"select status from mango_swarm_tasks where id = ?", String.class, taskId))
				.isEqualTo(TaskStatus.IN_PROGRESS.databaseValue());
		assertThat(jdbcTemplate.queryForObject(
						"select count(*) from mango_swarm_task_runtime where task_id = ?", Integer.class, taskId))
				.isEqualTo(1);
	}

	@Test
	void failedTaskRecordsRuntimeState() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);

		taskRepository.markFailed(taskId, workerId, now.plusSeconds(1), "remote error");

		var row = jdbcTemplate.queryForMap(
				"""
					select t.status, t.execution_time_ms task_execution_time_ms, r.worker_id, r.execution_state,
						r.progress_percent, r.progress_message, r.updated_at, r.execution_time_ms runtime_execution_time_ms
					from mango_swarm_tasks t
					join mango_swarm_task_runtime r on r.task_id = t.id
					where t.id = ?
				""",
				taskId);
		assertThat(row)
				.containsEntry("status", "failed")
				.containsEntry("task_execution_time_ms", 0L)
				.containsEntry("worker_id", workerId)
				.containsEntry("execution_state", "failed")
				.containsEntry("progress_percent", null)
				.containsEntry("progress_message", "remote error")
				.containsEntry("runtime_execution_time_ms", 0L);
		assertThat(((java.sql.Timestamp) row.get("updated_at")).toInstant()).isEqualTo(now.plusSeconds(1));
	}

	@Test
	void failureDoesNothingForDifferentWorker() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.markInProgress(taskId, workerId, now);

		taskRepository.markFailed(taskId, UUID.randomUUID(), now.plusSeconds(1), "wrong worker");

		var row = jdbcTemplate.queryForMap(
				"""
				select status, failed_at, last_error_message, execution_time_ms
				from mango_swarm_tasks
				where id = ?
				""",
				taskId);
		assertThat(row)
				.containsEntry("status", TaskStatus.IN_PROGRESS.databaseValue())
				.containsEntry("failed_at", null)
				.containsEntry("last_error_message", null)
				.containsEntry("execution_time_ms", 0L);
		assertThat(jdbcTemplate.queryForObject(
						"select execution_state from mango_swarm_task_runtime where task_id = ?", String.class, taskId))
				.isEqualTo("running");
	}

	@Test
	void markTimedOutFailedReturnsUpdatedRows() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
		taskRepository.claimBatch("email", workerId, now.minusSeconds(60), 1);
		taskRepository.markInProgress(taskId, workerId, now.minusSeconds(60));

		int failed = taskRepository.markTimedOutFailed("email", Duration.ofSeconds(30), now, 10);

		assertThat(failed).isEqualTo(1);
		assertThat(jdbcTemplate.queryForObject(
						"select status from mango_swarm_tasks where id = ?", String.class, taskId))
				.isEqualTo(TaskStatus.FAILED.databaseValue());
	}

	@Test
	void markTimedOutFailedUpdatesSingleTaskAtBatchLimitBoundary() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
		taskRepository.claimBatch("email", workerId, now.minusSeconds(60), 1);

		int failed = taskRepository.markTimedOutFailed("email", Duration.ofSeconds(30), now, 1);

		assertThat(failed).isEqualTo(1);
		assertThat(jdbcTemplate.queryForObject(
						"select status from mango_swarm_tasks where id = ?", String.class, taskId))
				.isEqualTo(TaskStatus.FAILED.databaseValue());
	}

	@Test
	void timeoutRecoveryUpdatesTasksInBatches() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID first = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
		UUID second = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
		taskRepository.claimBatch("email", workerId, now.minusSeconds(60), 2);
		taskRepository.markInProgress(first, workerId, now.minusSeconds(60));
		taskRepository.markInProgress(second, workerId, now.minusSeconds(60));

		int reclaimed = taskRepository.reclaimTimedOut("email", Duration.ofSeconds(30), now, 1);

		assertThat(reclaimed).isEqualTo(1);
		Integer queued = jdbcTemplate.queryForObject(
				"select count(*) from mango_swarm_tasks where status = 'queued'", Integer.class);
		assertThat(queued).isEqualTo(1);
	}

	@Test
	void zeroTimeoutRecoveryLimitUpdatesNothing() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now.minusSeconds(60));
		taskRepository.claimBatch("email", workerId, now.minusSeconds(60), 1);

		int reclaimed = taskRepository.reclaimTimedOut("email", Duration.ofSeconds(30), now, 0);

		assertThat(reclaimed).isZero();
		assertThat(jdbcTemplate.queryForObject(
						"select status from mango_swarm_tasks where id = ?", String.class, taskId))
				.isEqualTo(TaskStatus.CLAIMED.databaseValue());
	}

	@Test
	void deletesTerminalTasksOlderThanRetention() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID completedOld = completeTask(workerId, now.minus(Duration.ofDays(40)));
		UUID completedRecent = completeTask(workerId, now.minus(Duration.ofDays(5)));
		UUID failedOld = failTask(workerId, now.minus(Duration.ofDays(40)));
		UUID failedRecent = failTask(workerId, now.minus(Duration.ofDays(5)));

		int deletedCompleted = taskRepository.deleteCompletedOlderThan(Duration.ofDays(30), now, 10);
		int deletedFailed = taskRepository.deleteFailedOlderThan(Duration.ofDays(30), now, 10);

		assertThat(deletedCompleted).isEqualTo(1);
		assertThat(deletedFailed).isEqualTo(1);
		assertThat(existingTaskIds()).containsExactlyInAnyOrder(completedRecent, failedRecent);
		assertThat(existingTaskIds()).doesNotContain(completedOld, failedOld);
	}

	@Test
	void deletesOneFailedTaskAtBatchLimitBoundary() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID firstOld = failTask(workerId, now.minus(Duration.ofDays(40)));
		UUID secondOld = failTask(workerId, now.minus(Duration.ofDays(39)));

		int deleted = taskRepository.deleteFailedOlderThan(Duration.ofDays(30), now, 1);

		assertThat(deleted).isEqualTo(1);
		assertThat(existingTaskIds()).contains(secondOld);
		assertThat(existingTaskIds()).doesNotContain(firstOld);
	}

	@Test
	void cleanupDeletesTerminalTasksInBatches() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID firstOld = completeTask(workerId, now.minus(Duration.ofDays(40)));
		UUID secondOld = completeTask(workerId, now.minus(Duration.ofDays(39)));
		UUID recent = completeTask(workerId, now.minus(Duration.ofDays(5)));

		int deleted = taskRepository.deleteCompletedOlderThan(Duration.ofDays(30), now, 1);

		assertThat(deleted).isEqualTo(1);
		assertThat(existingTaskIds()).contains(secondOld, recent);
		assertThat(existingTaskIds()).doesNotContain(firstOld);
	}

	@Test
	void zeroCleanupLimitDeletesNothing() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID old = completeTask(workerId, now.minus(Duration.ofDays(40)));

		int deleted = taskRepository.deleteCompletedOlderThan(Duration.ofDays(30), now, 0);

		assertThat(deleted).isZero();
		assertThat(existingTaskIds()).contains(old);
	}

	@Test
	void maintenanceMethodsIgnoreNonPositiveLimits() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		assertThat(taskRepository.reclaimTimedOut("email", Duration.ofSeconds(30), now, 0))
				.isZero();
		assertThat(taskRepository.markTimedOutFailed("email", Duration.ofSeconds(30), now, 0))
				.isZero();
		assertThat(taskRepository.deleteCompletedOlderThan(Duration.ofDays(30), now, 0))
				.isZero();
		assertThat(taskRepository.deleteFailedOlderThan(Duration.ofDays(30), now, 0))
				.isZero();
	}

	@Test
	void longMessagesAreTruncated() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		String longMessage = "x".repeat(4_100);

		taskRepository.requeueClaimed(taskId, workerId, now, now, longMessage);

		String stored = jdbcTemplate.queryForObject(
				"select last_error_message from mango_swarm_tasks where id = ?", String.class, taskId);
		assertThat(stored).hasSize(4_000).isEqualTo("x".repeat(4_000));
	}

	@Test
	void requeueClaimedDeletesRuntimeRow() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.updateRuntime(taskId, workerId, now, "running", 10, "started");

		taskRepository.requeueClaimed(taskId, workerId, now.plusSeconds(1), now.plusSeconds(10), "capacity");

		assertThat(jdbcTemplate.queryForObject(
						"select count(*) from mango_swarm_task_runtime where task_id = ?", Integer.class, taskId))
				.isZero();
	}

	@Test
	void requeueClaimedDoesNothingForDifferentWorker() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.updateRuntime(taskId, workerId, now, "running", 10, "started");

		taskRepository.requeueClaimed(
				taskId, UUID.randomUUID(), now.plusSeconds(1), now.plusSeconds(10), "wrong worker");

		var row = jdbcTemplate.queryForMap(
				"""
				select status, claimed_by, last_error_message
				from mango_swarm_tasks
				where id = ?
				""",
				taskId);
		assertThat(row)
				.containsEntry("status", TaskStatus.CLAIMED.databaseValue())
				.containsEntry("claimed_by", workerId)
				.containsEntry("last_error_message", null);
		assertThat(jdbcTemplate.queryForObject(
						"select count(*) from mango_swarm_task_runtime where task_id = ?", Integer.class, taskId))
				.isEqualTo(1);
	}

	@Test
	void nullAndShortMessagesAreStoredAsProvided() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID nullMessageTask = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.markFailed(nullMessageTask, workerId, now, null);

		UUID shortMessageTask = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		taskRepository.markFailed(shortMessageTask, workerId, now, "short");

		assertThat(jdbcTemplate.queryForObject(
						"select last_error_message from mango_swarm_tasks where id = ?", String.class, nullMessageTask))
				.isNull();
		assertThat(jdbcTemplate.queryForObject(
						"select last_error_message from mango_swarm_tasks where id = ?",
						String.class,
						shortMessageTask))
				.isEqualTo("short");
	}

	@Test
	void exactLimitMessagesAreNotTruncated() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID workerId = UUID.randomUUID();
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), now);
		taskRepository.claimBatch("email", workerId, now, 1);
		String exactLimitMessage = "x".repeat(4_000);

		taskRepository.markFailed(taskId, workerId, now, exactLimitMessage);

		assertThat(jdbcTemplate.queryForObject(
						"select last_error_message from mango_swarm_tasks where id = ?", String.class, taskId))
				.isEqualTo(exactLimitMessage);
	}

	@Test
	void postgresJsonPayloadsAreBoundAsJsonbObjects() throws Exception {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		java.sql.Connection connection = mock(java.sql.Connection.class);
		java.sql.DatabaseMetaData metaData = mock(java.sql.DatabaseMetaData.class);
		when(connection.getMetaData()).thenReturn(metaData);
		when(metaData.getDatabaseProductName()).thenReturn("PostgreSQL");
		when(jdbcTemplate.execute(org.mockito.ArgumentMatchers.<ConnectionCallback<Boolean>>any()))
				.thenAnswer(invocation ->
						invocation.<ConnectionCallback<Boolean>>getArgument(0).doInConnection(connection));
		JdbcTaskRepository repository =
				new JdbcTaskRepository(jdbcTemplate, objectMapper, new SchemaQualifiedTables(null));
		PreparedStatement statement = mock(PreparedStatement.class);

		var method = JdbcTaskRepository.class.getDeclaredMethod(
				"setJson", PreparedStatement.class, com.fasterxml.jackson.databind.JsonNode.class);
		method.setAccessible(true);
		method.invoke(
				repository, statement, JsonNodeFactory.instance.objectNode().put("subject", "hello"));
		method.invoke(
				repository, statement, JsonNodeFactory.instance.objectNode().put("subject", "hello"));

		var captor = org.mockito.ArgumentCaptor.forClass(PGobject.class);
		verify(statement, org.mockito.Mockito.times(2)).setObject(eq(3), captor.capture(), eq(java.sql.Types.OTHER));
		verify(jdbcTemplate).execute(org.mockito.ArgumentMatchers.<ConnectionCallback<Boolean>>any());
		assertThat(captor.getValue().getType()).isEqualTo("jsonb");
		assertThat(captor.getValue().getValue()).isEqualTo("{\"subject\":\"hello\"}");
	}

	@Test
	void rejectsNullJdbcTemplateExecuteResult() {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		JdbcTaskRepository repository =
				new JdbcTaskRepository(jdbcTemplate, objectMapper, new SchemaQualifiedTables(null));
		when(jdbcTemplate.execute(org.mockito.ArgumentMatchers.<ConnectionCallback<UUID>>any()))
				.thenReturn(null);
		var payload = JsonNodeFactory.instance.objectNode();
		Instant availableAt = Instant.parse("2026-05-20T10:00:00Z");

		assertThatThrownBy(() -> repository.queue("email", payload, availableAt))
				.isInstanceOf(NullPointerException.class)
				.hasMessage("JdbcTemplate.execute returned null for queue task");
	}

	@Test
	void voidLifecycleCallbacksReturnUpdatedRowCount() throws Exception {
		assertLifecycleCallbackReturnsOne(repository ->
				repository.markInProgress(UUID.randomUUID(), UUID.randomUUID(), Instant.parse("2026-05-20T10:00:00Z")));
		assertLifecycleCallbackReturnsOne(repository -> repository.updateRuntime(
				UUID.randomUUID(), UUID.randomUUID(), Instant.parse("2026-05-20T10:00:00Z"), "running", 50, "halfway"));
		assertLifecycleCallbackReturnsOne(repository -> repository.recordProgress(
				UUID.randomUUID(), UUID.randomUUID(), Instant.parse("2026-05-20T10:00:00Z"), 50, "halfway"));
		assertLifecycleCallbackReturnsOne(repository ->
				repository.markCompleted(UUID.randomUUID(), UUID.randomUUID(), Instant.parse("2026-05-20T10:00:00Z")));
		assertLifecycleCallbackReturnsOne(repository -> repository.markFailed(
				UUID.randomUUID(), UUID.randomUUID(), Instant.parse("2026-05-20T10:00:00Z"), "failed"));
		assertLifecycleCallbackReturnsOne(repository -> repository.rescheduleAfterFailure(
				UUID.randomUUID(),
				UUID.randomUUID(),
				Instant.parse("2026-05-20T10:00:00Z"),
				Instant.parse("2026-05-20T10:01:00Z"),
				"retry"));
		assertLifecycleCallbackReturnsOne(repository -> repository.requeueClaimed(
				UUID.randomUUID(),
				UUID.randomUUID(),
				Instant.parse("2026-05-20T10:00:00Z"),
				Instant.parse("2026-05-20T10:01:00Z"),
				"capacity"));
	}

	private UUID completeTask(UUID workerId, Instant completedAt) {
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), completedAt);
		taskRepository.claimBatch("email", workerId, completedAt, 1);
		taskRepository.markCompleted(taskId, workerId, completedAt);
		return taskId;
	}

	private UUID failTask(UUID workerId, Instant failedAt) {
		UUID taskId = taskRepository.queue("email", JsonNodeFactory.instance.objectNode(), failedAt);
		taskRepository.claimBatch("email", workerId, failedAt, 1);
		taskRepository.markFailed(taskId, workerId, failedAt, "failed");
		return taskId;
	}

	private List<UUID> existingTaskIds() {
		return jdbcTemplate.query("select id from mango_swarm_tasks", (rs, rowNum) -> rs.getObject("id", UUID.class));
	}

	private void assertLifecycleCallbackReturnsOne(java.util.function.Consumer<JdbcTaskRepository> operation)
			throws Exception {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		java.sql.Connection connection = mock(java.sql.Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
				.thenReturn(statement);
		when(statement.executeUpdate()).thenReturn(1);
		when(statement.executeQuery()).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(false);
		when(jdbcTemplate.execute(org.mockito.ArgumentMatchers.<ConnectionCallback<Integer>>any()))
				.thenAnswer(invocation -> {
					Integer result = invocation
							.<ConnectionCallback<Integer>>getArgument(0)
							.doInConnection(connection);
					assertThat(result).isEqualTo(1);
					return result;
				});
		JdbcTaskRepository repository =
				new JdbcTaskRepository(jdbcTemplate, objectMapper, new SchemaQualifiedTables(null));
		setH2(repository);

		operation.accept(repository);
	}

	private static void setH2(JdbcTaskRepository repository) throws Exception {
		java.lang.reflect.Field h2 = JdbcTaskRepository.class.getDeclaredField("h2");
		java.lang.reflect.Field h2Initialized = JdbcTaskRepository.class.getDeclaredField("h2Initialized");
		h2.setAccessible(true);
		h2Initialized.setAccessible(true);
		h2.setBoolean(repository, true);
		h2Initialized.setBoolean(repository, true);
	}

	private static ResultSet resultSet(Map<String, Object> row) {
		return (ResultSet) Proxy.newProxyInstance(
				ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class}, (proxy, method, args) -> {
					String methodName = method.getName();
					if ("getTimestamp".equals(methodName)
							|| "getString".equals(methodName)
							|| "getInt".equals(methodName)
							|| "getObject".equals(methodName)) {
						return row.get((String) args[0]);
					}
					throw new UnsupportedOperationException(methodName);
				});
	}
}
