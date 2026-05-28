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
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ie.bitstep.mango.swarm.H2TestSupport;
import ie.bitstep.mango.swarm.TaskStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
				.isEqualTo(TaskStatus.queued.name());
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
		row.put("status", TaskStatus.queued.name());
		row.put("available_at", Timestamp.from(availableAt));
		row.put("claimed_by", workerId);
		row.put("claimed_at", null);
		row.put("attempt_count", 0);
		row.put("created_at", Timestamp.from(createdAt));
		row.put("updated_at", Timestamp.from(updatedAt));

		TaskRecord record = ((JdbcTaskRepository) taskRepository).mapTask(resultSet(row), 0);

		assertThat(record.id()).isEqualTo(taskId);
		assertThat(record.taskType()).isEqualTo("email");
		assertThat(record.status()).isEqualTo(TaskStatus.queued);
		assertThat(record.availableAt()).isEqualTo(availableAt);
		assertThat(record.claimedBy()).isEqualTo(workerId);
		assertThat(record.claimedAt()).isNull();
		assertThat(record.attemptCount()).isZero();
		assertThat(record.createdAt()).isEqualTo(createdAt);
		assertThat(record.updatedAt()).isEqualTo(updatedAt);
		assertThat(record.payload().get("subject").asText()).isEqualTo("hello");
	}

	@Test
	void mapsClaimedTaskRowsWithClaimTimestamp() throws Exception {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		Map<String, Object> row = new HashMap<>();
		row.put("id", UUID.randomUUID());
		row.put("task_type", "email");
		row.put("payload", "{}");
		row.put("status", TaskStatus.claimed.name());
		row.put("available_at", Timestamp.from(now.minusSeconds(1)));
		row.put("claimed_by", UUID.randomUUID());
		row.put("claimed_at", Timestamp.from(now));
		row.put("attempt_count", 1);
		row.put("created_at", Timestamp.from(now.minusSeconds(2)));
		row.put("updated_at", Timestamp.from(now));

		TaskRecord record = ((JdbcTaskRepository) taskRepository).mapTask(resultSet(row), 0);

		assertThat(record.claimedAt()).isEqualTo(now);
	}

	@Test
	void queueInNextSlotPushesRequestedTimePastLatestSlot() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID first = taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode().put("i", 1), now, java.time.Duration.ofMillis(10));
		UUID second = taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode().put("i", 2), now, java.time.Duration.ofMillis(10));
		UUID third = taskRepository.queueInNextSlot(
				"email",
				JsonNodeFactory.instance.objectNode().put("i", 3),
				now.plusSeconds(1),
				java.time.Duration.ofMillis(10));

		var rows = jdbcTemplate.queryForList(
				"""
				select id, available_at
				from mango_swarm_tasks
				order by available_at
				""");
		assertThat(rows).hasSize(3);
		assertThat(rows.get(0).get("id")).isEqualTo(first);
		assertThat(((java.sql.Timestamp) rows.get(0).get("available_at")).toInstant())
				.isEqualTo(now);
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
				"email", JsonNodeFactory.instance.objectNode().put("i", "now"), now, java.time.Duration.ofMillis(10));
		UUID beforeFuture = taskRepository.queueInNextSlot(
				"email",
				JsonNodeFactory.instance.objectNode().put("i", "before-future"),
				future.minusSeconds(1),
				java.time.Duration.ofMillis(10));

		var rows = jdbcTemplate.queryForList(
				"""
				select id, available_at
				from mango_swarm_tasks
				where id in (?, ?)
				order by available_at
				""",
				immediate,
				beforeFuture);
		assertThat(((java.sql.Timestamp) rows.get(0).get("available_at")).toInstant())
				.isEqualTo(now);
		assertThat(((java.sql.Timestamp) rows.get(1).get("available_at")).toInstant())
				.isEqualTo(future.minusSeconds(1));
	}

	@Test
	void exactSlotSpacingBoundaryIsAvailable() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode().put("i", 1), now, Duration.ofMillis(10));

		UUID boundary = taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode().put("i", 2), now.plusMillis(10), Duration.ofMillis(10));

		Instant availableAt = jdbcTemplate.queryForObject(
				"select available_at from mango_swarm_tasks where id = ?", Instant.class, boundary);
		assertThat(availableAt).isEqualTo(now.plusMillis(10));
	}

	@Test
	void queueInNextSlotParticipatesInExistingTransaction() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		TransactionTemplate transactionTemplate =
				new TransactionTemplate(new DataSourceTransactionManager(jdbcTemplate.getDataSource()));

		UUID taskId = transactionTemplate.execute(status -> taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode().put("i", "transactional"), now, Duration.ofMillis(10)));

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
				select status, progress_percent, progress_description, last_progress_at
				from mango_swarm_tasks
				where id = ?
				""",
				taskId);
		assertThat(row.get("status")).isEqualTo("in_progress");
		assertThat(row.get("progress_percent")).isEqualTo(50);
		assertThat(row.get("progress_description")).isEqualTo("sending");
		assertThat(((java.sql.Timestamp) row.get("last_progress_at")).toInstant())
				.isEqualTo(now.minusSeconds(5));
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
				select status, progress_percent, progress_description, last_progress_at
				from mango_swarm_tasks
				where id = ?
				""",
				taskId);
		assertThat(row.get("status")).isEqualTo("completed");
		assertThat(row.get("progress_percent")).isEqualTo(100);
		assertThat(row.get("progress_description")).isEqualTo("finished");
		assertThat(((java.sql.Timestamp) row.get("last_progress_at")).toInstant())
				.isEqualTo(now.plusSeconds(5));
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
				select status, available_at, claimed_by, claimed_at, failed_at, last_error_message
				from mango_swarm_tasks
				where id = ?
				""",
				taskId);
		assertThat(row.get("status")).isEqualTo("queued");
		assertThat(((java.sql.Timestamp) row.get("available_at")).toInstant()).isEqualTo(retryAt);
		assertThat(row.get("claimed_by")).isNull();
		assertThat(row.get("claimed_at")).isNull();
		assertThat(row.get("failed_at")).isNull();
		assertThat(row.get("last_error_message")).isEqualTo("temporary failure");
	}

	@Test
	void zeroOrNegativeSlotSpacingQueuesAtRequestedTime() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		UUID zero = taskRepository.queueInNextSlot("email", JsonNodeFactory.instance.objectNode(), now, Duration.ZERO);
		UUID negative = taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode(), now.plusSeconds(1), Duration.ofMillis(-1));

		var rows = jdbcTemplate.queryForList(
				"""
				select id, available_at
				from mango_swarm_tasks
				where id in (?, ?)
				order by available_at
				""",
				zero,
				negative);
		assertThat(((java.sql.Timestamp) rows.get(0).get("available_at")).toInstant())
				.isEqualTo(now);
		assertThat(((java.sql.Timestamp) rows.get(1).get("available_at")).toInstant())
				.isEqualTo(now.plusSeconds(1));
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
				.isEqualTo(TaskStatus.failed.name());
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
				.isEqualTo(TaskStatus.failed.name());
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
				.isEqualTo(TaskStatus.claimed.name());
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
	void deletesOldTaskPacerSlotsInBatches() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode(), now.minus(Duration.ofDays(40)), Duration.ofMillis(10));
		taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode(), now.minus(Duration.ofDays(39)), Duration.ofMillis(10));
		taskRepository.queueInNextSlot(
				"email", JsonNodeFactory.instance.objectNode(), now.minus(Duration.ofDays(5)), Duration.ofMillis(10));

		int deleted = taskRepository.deleteTaskPacersOlderThan(Duration.ofDays(30), now, 1);

		assertThat(deleted).isEqualTo(1);
		Integer remaining = jdbcTemplate.queryForObject("select count(*) from mango_swarm_task_pacers", Integer.class);
		assertThat(remaining).isEqualTo(2);
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
		JdbcTaskRepository repository = (JdbcTaskRepository) taskRepository;
		var h2 = JdbcTaskRepository.class.getDeclaredField("h2");
		h2.setAccessible(true);
		h2.set(repository, false);
		PreparedStatement statement = mock(PreparedStatement.class);

		var method = JdbcTaskRepository.class.getDeclaredMethod(
				"setJson", PreparedStatement.class, int.class, com.fasterxml.jackson.databind.JsonNode.class);
		method.setAccessible(true);
		method.invoke(
				repository, statement, 3, JsonNodeFactory.instance.objectNode().put("subject", "hello"));

		var captor = org.mockito.ArgumentCaptor.forClass(PGobject.class);
		verify(statement).setObject(eq(3), captor.capture(), eq(java.sql.Types.OTHER));
		assertThat(captor.getValue().getType()).isEqualTo("jsonb");
		assertThat(captor.getValue().getValue()).isEqualTo("{\"subject\":\"hello\"}");
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
