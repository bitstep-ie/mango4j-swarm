package ie.bitstep.mango.swarm.executor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;
import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRecord;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.handler.SwarmHandler;
import ie.bitstep.mango.swarm.handler.TaskHandler;
import ie.bitstep.mango.swarm.handler.TaskHandlerException;
import ie.bitstep.mango.swarm.handler.TaskHandlerRegistry;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class MangoSwarmDaemonTest {

	@Test
	void startIsIdempotentAndStopInterruptsSleepingLoop() throws Exception {
		MangoSwarmProperties properties = properties(0, 1, 1);
		properties.getExecutor().setPollInterval(Duration.ofSeconds(30));
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		daemon.start();
		daemon.start();
		Thread daemonThread = (Thread) fieldValue(daemon, "daemonThread");
		assertThat(daemonThread.isAlive()).isTrue();
		daemon.stop();

		assertThat(((AtomicBoolean) fieldValue(daemon, "running")).get()).isFalse();
		assertThat(repository.claimLimits).isEmpty();
	}

	@Test
	void dividesRateByActiveWorkerCountForBatchAcquisition() {
		MangoSwarmProperties properties = properties(100, 50, 50);
		properties.getExecutor().setMaxThreads("100");
		FakeRepository repository = new FakeRepository(100);
		MangoSwarmDaemon daemon = daemon(properties, repository, 4, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		daemon.pollOnce(now);

		assertThat(repository.lastClaimLimit).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void respectsConfiguredBatchAndConcurrencyAndExecutorCapacity() {
		MangoSwarmProperties properties = properties(100, 3, 20);
		properties.getExecutor().setMaxThreads("2");
		FakeRepository repository = new FakeRepository(100);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(repository.lastClaimLimit).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void immediatelyPollsAgainAfterNonEmptyClaim() {
		MangoSwarmProperties properties = properties(100, 5, 5);
		FakeRepository repository = new FakeRepository(100);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		Duration delay = daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(delay).isEqualTo(Duration.ZERO);
		assertThat(repository.lastClaimLimit).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void eligibleOverdueTasksDoNotAllStartImmediately() throws Exception {
		MangoSwarmProperties properties = properties(1, 10, 10);
		properties.getExecutor().setMaxThreads("10");
		FakeRepository repository = new FakeRepository(10);
		CountDownLatch executed = new CountDownLatch(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CountingCompletingHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.completedTaskCalls, 1);
		assertThat(repository.claimLimits).containsExactly(1);
		assertThat(repository.markInProgressCalls).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void startsArePacedByLocalTokenRing() throws Exception {
		MangoSwarmProperties properties = properties(1, 10, 10);
		properties.getExecutor().setMaxThreads("10");
		FakeRepository repository = new FakeRepository(10);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		daemon.pollOnce(now);
		daemon.pollOnce(now.plusMillis(500));
		daemon.pollOnce(now.plusSeconds(1));

		awaitCounter(() -> repository.completedTaskCalls, 2);
		assertThat(repository.claimLimits).containsExactly(1, 1);
		assertThat(repository.markInProgressCalls).isEqualTo(2);
		daemon.stop();
	}

	@Test
	void largeBacklogStartsAtConfiguredRateWhenTaskTypeExecutes() throws Exception {
		MangoSwarmProperties properties = properties(5, 20, 20);
		properties.getExecutor().setMaxThreads("20");
		FakeRepository repository = new FakeRepository(100);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		for (int tick = 0; tick <= 40; tick++) {
			daemon.pollOnce(now.plusMillis(tick * 50L));
		}

		awaitCounter(() -> repository.completedTaskCalls, 11);
		assertThat(repository.claimInstants.stream()
						.filter(instant -> instant.isBefore(now.plusSeconds(1)))
						.count())
				.isEqualTo(5);
		assertThat(repository.claimInstants.stream()
						.filter(instant -> instant.isBefore(now.plusSeconds(2)))
						.count())
				.isEqualTo(10);
		assertThat(repository.claimInstants).hasSize(11);
		for (int i = 1; i < repository.claimInstants.size(); i++) {
			assertThat(Duration.between(repository.claimInstants.get(i - 1), repository.claimInstants.get(i)))
					.isGreaterThanOrEqualTo(Duration.ofMillis(200));
		}
		daemon.stop();
	}

	@Test
	void waitsForFutureTokenAcrossTheBatchWindow() {
		MangoSwarmProperties properties = properties(1, 10, 5);
		properties.getTaskTypes().get("email").setPeriod(Duration.ofSeconds(3));
		FakeRepository repository = new FakeRepository(10);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		daemon.pollOnce(now);
		daemon.pollOnce(now.plusMillis(250));

		assertThat(repository.claimLimits).containsExactly(1);
		daemon.stop();
	}

	@Test
	void enforcesPerTaskConcurrencyAcrossPolls() throws Exception {
		MangoSwarmProperties properties = properties(100, 1, 10);
		properties.getExecutor().setMaxThreads("4");
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		BlockingHandler handler = new BlockingHandler(entered, release);
		FakeRepository repository = new FakeRepository(10);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, handler);
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		daemon.pollOnce(now);
		assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
		daemon.pollOnce(now.plusMillis(10));

		assertThat(repository.claimLimits).containsExactly(1);
		release.countDown();
		daemon.stop();
	}

	@Test
	void reclaimsTimedOutOnlyForIdempotentTypes() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setReclaimOnTimeout(true);
		properties.getTaskTypes().get("email").setIdempotent(false);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(repository.reclaimCalls).isZero();
		assertThat(repository.failTimedOutCalls).isZero();
		daemon.stop();
	}

	@Test
	void queuedTaskTypesAreNotClaimedOrRecovered() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setMode(MangoSwarmProperties.TaskMode.QUEUE);
		FakeRepository repository = new FakeRepository(10);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		Duration delay = daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(delay).isEqualTo(properties.getExecutor().getPollInterval());
		assertThat(repository.claimLimits).isEmpty();
		assertThat(repository.reclaimCalls).isZero();
		assertThat(repository.failTimedOutCalls).isZero();
		daemon.stop();
	}

	@Test
	void marksTimedOutFailedWhenReclaimDisabled() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setReclaimOnTimeout(false);
		properties.getCleanup().setBatchSize(25);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(repository.failTimedOutCalls).isEqualTo(1);
		assertThat(repository.lastTimeoutRecoveryLimit).isEqualTo(25);
		daemon.stop();
	}

	@Test
	void throttlesTimeoutRecoveryScans() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setReclaimOnTimeout(false);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		daemon.pollOnce(now);
		daemon.pollOnce(now.plusMillis(500));
		daemon.pollOnce(now.plusSeconds(1));

		assertThat(repository.failTimedOutCalls).isEqualTo(2);
		daemon.stop();
	}

	@Test
	void backsOffPollingWhenQueueIsEmpty() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getExecutor().setPollInterval(Duration.ofMillis(100));
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		Duration first = daemon.pollOnce(now);
		Duration second = daemon.pollOnce(now.plusMillis(100));
		Duration third = daemon.pollOnce(now.plusMillis(200));
		Duration fourth = daemon.pollOnce(now.plusMillis(300));

		assertThat(first).isEqualTo(Duration.ofMillis(100));
		assertThat(second).isEqualTo(Duration.ofMillis(200));
		assertThat(third).isEqualTo(Duration.ofMillis(400));
		assertThat(fourth).isEqualTo(Duration.ofMillis(800));
		daemon.stop();
	}

	@Test
	void emptyClaimBackoffResetsOnlyAfterNonEmptyClaim() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getExecutor().setPollInterval(Duration.ofMillis(100));
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		Duration first = daemon.pollOnce(now);
		Duration second = daemon.pollOnce(now.plusMillis(100));
		repository.setAvailable(1);
		Duration afterWork = daemon.pollOnce(now.plusMillis(200));
		repository.setAvailable(0);
		Duration reset = daemon.pollOnce(now.plusMillis(300));

		assertThat(first).isEqualTo(Duration.ofMillis(100));
		assertThat(second).isEqualTo(Duration.ofMillis(200));
		assertThat(afterWork).isEqualTo(Duration.ZERO);
		assertThat(reset).isEqualTo(Duration.ofMillis(100));
		daemon.stop();
	}

	@Test
	void runsCleanupUsingConfiguredRetentions() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getCleanup().setInterval(Duration.ofMinutes(1));
		properties.getCleanup().setCompletedRetention(Duration.ofDays(30));
		properties.getCleanup().setFailedRetention(Duration.ofDays(90));
		properties.getCleanup().setBatchSize(25);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));
		daemon.pollOnce(Instant.parse("2026-05-20T10:00:30Z"));
		daemon.pollOnce(Instant.parse("2026-05-20T10:01:00Z"));

		assertThat(repository.deleteCompletedCalls).isEqualTo(2);
		assertThat(repository.deleteFailedCalls).isEqualTo(2);
		assertThat(repository.lastDeleteLimit).isEqualTo(25);
		daemon.stop();
	}

	@Test
	void skipsCleanupWhenDisabledOrIntervalIsInvalid() {
		MangoSwarmProperties disabled = properties(10, 1, 1);
		disabled.getCleanup().setEnabled(false);
		FakeRepository disabledRepository = new FakeRepository(0);
		MangoSwarmDaemon disabledDaemon = daemon(disabled, disabledRepository, 1, new CompletingHandler());

		disabledDaemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));
		disabledDaemon.stop();

		MangoSwarmProperties invalidInterval = properties(10, 1, 1);
		invalidInterval.getCleanup().setInterval(Duration.ZERO);
		FakeRepository invalidIntervalRepository = new FakeRepository(0);
		MangoSwarmDaemon invalidIntervalDaemon =
				daemon(invalidInterval, invalidIntervalRepository, 1, new CompletingHandler());

		invalidIntervalDaemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));
		invalidIntervalDaemon.stop();

		assertThat(disabledRepository.deleteCompletedCalls).isZero();
		assertThat(disabledRepository.deleteFailedCalls).isZero();
		assertThat(invalidIntervalRepository.deleteCompletedCalls).isZero();
		assertThat(invalidIntervalRepository.deleteFailedCalls).isZero();
	}

	@Test
	void skipsCleanupWhenRetentionsAreNotPositive() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getCleanup().setInterval(Duration.ofSeconds(1));
		properties.getCleanup().setCompletedRetention(null);
		properties.getCleanup().setFailedRetention(Duration.ZERO);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(repository.deleteCompletedCalls).isZero();
		assertThat(repository.deleteFailedCalls).isZero();
		daemon.stop();
	}

	@Test
	void calculatesDerivedBatchSizeFromRateAndConcurrency() {
		MangoSwarmProperties properties = properties(3, 10, 1);
		MangoSwarmProperties.TaskType config = properties.getTaskTypes().get("email");
		config.setBatchSize(null);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		int claimLimit = daemon.calculateBatchSize("email", config, 3.2d, now);

		assertThat(claimLimit).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void batchDecisionReportsEachCapacityInput() throws Exception {
		MangoSwarmProperties properties = properties(3, 10, 1);
		properties.getExecutor().setMaxThreads("6");
		MangoSwarmProperties.TaskType config = properties.getTaskTypes().get("email");
		config.setBatchSize(null);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

		Object decision = invokeInstance(
				daemon,
				"decideBatchSize",
				new Class<?>[] {String.class, MangoSwarmProperties.TaskType.class, double.class, Instant.class},
				"email",
				config,
				3.2d,
				Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(invokeAccessor(decision, "configuredBatch")).isEqualTo(4);
		assertThat(invokeAccessor(decision, "startCapacity")).isEqualTo(1);
		assertThat(invokeAccessor(decision, "remainingTypeCapacity")).isEqualTo(10);
		assertThat(invokeAccessor(decision, "remainingExecutorCapacity")).isEqualTo(6);
		assertThat(invokeAccessor(decision, "claimLimit")).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void retriesFailedTaskWhenAttemptsRemain() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setMaxAttempts(3);
		properties.getTaskTypes().get("email").setRetryDelay(Duration.ofSeconds(5));
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new FailingHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.rescheduleAfterFailureCalls, 1);
		assertThat(repository.rescheduleAfterFailureCalls).isEqualTo(1);
		assertThat(repository.failedTaskCalls).isZero();
		assertThat(repository.retryAvailableAt).isNotNull();
		daemon.stop();
	}

	@Test
	void failsTaskWhenMaxAttemptsReached() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setMaxAttempts(1);
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new FailingHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.failedTaskCalls, 1);
		assertThat(repository.rescheduleAfterFailureCalls).isZero();
		assertThat(repository.failedTaskCalls).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void handlerProgressIsRecordedAsTaskLiveness() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new ProgressHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.progressCalls, 2);
		assertThat(repository.progressCalls).isEqualTo(2);
		assertThat(repository.lastProgressPercent).isEqualTo(100);
		assertThat(repository.lastProgressDescription).isEqualTo("completed");
		daemon.stop();
	}

	@Test
	void handlerProgressIsThrottledByThreshold() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new StableProgressHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.progressCalls, 2);
		assertThat(repository.progressCalls).isEqualTo(2);
		assertThat(repository.lastProgressPercent).isEqualTo(35);
		assertThat(repository.lastProgressDescription).isEqualTo("same");
		daemon.stop();
	}

	@Test
	void duplicateHandlerProgressIsNotPersistedTwice() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new DuplicateProgressHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.progressCalls, 1);
		assertThat(repository.progressCalls).isEqualTo(1);
		assertThat(repository.lastProgressPercent).isEqualTo(25);
		assertThat(repository.lastProgressDescription).isEqualTo("same");
		daemon.stop();
	}

	@Test
	void runtimeProgressReporterPersistsOnlyChangedNullProgress() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getRuntime().setMinUpdateInterval(Duration.ofDays(1));
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Object reporter = newRuntimeProgressReporter(daemon);

		invokeReport(reporter, "running", null, null);
		invokeReport(reporter, "running", null, null);
		invokeReport(reporter, "running", 25, null);
		invokeReport(reporter, "running", null, null);

		assertThat(repository.progressCalls).isEqualTo(3);
		assertThat(repository.lastProgressPercent).isEqualTo(-1);
		daemon.stop();
	}

	@Test
	void completedTaskIsMarkedInProgressAndCompleted() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getExecutor().setMaxThreads("1");
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CountingCompletingHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.completedTaskCalls, 1);
		assertThat(repository.markInProgressCalls).isEqualTo(1);
		assertThat(repository.completedTaskCalls).isEqualTo(1);
		assertThat(((TaskConcurrencyTracker) fieldValue(daemon, "taskConcurrencyTracker")).remaining("email"))
				.isEqualTo(1);
		assertThat(((Semaphore) fieldValue(daemon, "executorCapacity")).availablePermits())
				.isEqualTo(1);
		daemon.stop();
	}

	@Test
	void nullHandlerResultCompletesTaskAndPayloadComesFromExtractor() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new NullResultHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.completedTaskCalls, 1);
		assertThat(repository.completedTaskCalls).isEqualTo(1);
		daemon.stop();
	}

	@Test
	void handlerExceptionsFailTaskWhenNoAttemptsRemain() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setMaxAttempts(1);
		CountDownLatch executed = new CountDownLatch(1);
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new ThrowingHandler(executed));

		daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
		awaitCounter(() -> repository.failedTaskCalls, 1);
		assertThat(repository.failedTaskCalls).isEqualTo(1);
		assertThat(repository.lastFailureMessage).isEqualTo("boom");
		daemon.stop();
	}

	@Test
	void dispatchRequeuesWhenLocalTaskCapacityIsUnavailable() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		TaskConcurrencyTracker tracker = (TaskConcurrencyTracker) fieldValue(daemon, "taskConcurrencyTracker");
		assertThat(tracker.tryAcquire("email")).isTrue();
		TaskRecord task = taskRecord("email", Instant.parse("2026-05-20T10:00:00Z"));

		Object dispatchDecision = invokeInstance(
				daemon,
				"dispatch",
				new Class<?>[] {TaskRecord.class, Instant.class},
				task,
				Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(invokeAccessor(dispatchDecision, "dispatched")).isEqualTo(false);
		assertThat(repository.requeueCalls).isEqualTo(1);
		assertThat(repository.lastRequeueReason).contains("task-type concurrency");
		tracker.release("email");
		daemon.stop();
	}

	@Test
	void dispatchRequeuesAndReleasesTypeCapacityWhenExecutorCapacityIsUnavailable() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getExecutor().setMaxThreads("1");
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Semaphore executorCapacity = (Semaphore) fieldValue(daemon, "executorCapacity");
		assertThat(executorCapacity.tryAcquire()).isTrue();
		TaskRecord task = taskRecord("email", Instant.parse("2026-05-20T10:00:00Z"));

		Object dispatchDecision = invokeInstance(
				daemon,
				"dispatch",
				new Class<?>[] {TaskRecord.class, Instant.class},
				task,
				Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(invokeAccessor(dispatchDecision, "dispatched")).isEqualTo(false);
		assertThat(repository.requeueCalls).isEqualTo(1);
		assertThat(repository.lastRequeueReason).contains("executor capacity");
		assertThat(((TaskConcurrencyTracker) fieldValue(daemon, "taskConcurrencyTracker")).remaining("email"))
				.isEqualTo(1);
		executorCapacity.release();
		daemon.stop();
	}

	@Test
	void pollWaitsConfiguredIntervalWhenClaimedTaskCannotBeDispatched() throws Exception {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getExecutor().setMaxThreads("1");
		properties.getExecutor().setPollInterval(Duration.ofMillis(250));
		FakeRepository repository = new FakeRepository(1);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Semaphore executorCapacity = (Semaphore) fieldValue(daemon, "executorCapacity");
		repository.afterClaimLimitCalculated =
				() -> assertThat(executorCapacity.tryAcquire()).isTrue();

		Duration delay = daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

		assertThat(delay).isEqualTo(Duration.ofMillis(250));
		assertThat(repository.requeueCalls).isEqualTo(1);
		executorCapacity.release();
		daemon.stop();
	}

	@Test
	void skipsHeartbeatUntilIntervalHasElapsed() {
		MangoSwarmProperties properties = properties(10, 1, 1);
		properties.getTaskTypes().get("email").setBatchSize(null);
		properties.getTaskTypes().get("email").setConcurrency(10);
		properties.getWorker().setHeartbeatInterval(Duration.ofSeconds(10));
		TestWorkerRegistry workers = new TestWorkerRegistry(3);
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, workers, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		daemon.pollOnce(now);
		daemon.pollOnce(now.plusSeconds(9));
		daemon.pollOnce(now.plusSeconds(10));

		assertThat(workers.heartbeatCalls).isEqualTo(2);
		assertThat(repository.claimLimits).containsExactly(1, 1);
		daemon.stop();
	}

	@Test
	void returnsRateLimiterDelayWhenNoPermitIsCurrentlyAvailable() {
		MangoSwarmProperties properties = properties(1, 1, 1);
		properties.getTaskTypes().get("email").setPeriod(Duration.ofSeconds(1));
		FakeRepository repository = new FakeRepository(0);
		MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		assertThat(daemon.pollOnce(now)).isEqualTo(Duration.ofMillis(100));
		Duration rateLimitedDelay = daemon.pollOnce(now.minusMillis(100));

		assertThat(rateLimitedDelay).isEqualTo(Duration.ofMillis(100));
		assertThat(repository.claimLimits).containsExactly(1);
		daemon.stop();
	}

	@Test
	void calculatesExponentialRetryDelayFromGlobalDefaults() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
		properties.getRetry().setMultiplier(3.0d);
		MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();

		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(5));
		assertThat(MangoSwarmDaemon.retryDelay(2, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(15));
		assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(45));
	}

	@Test
	void taskRetryBackoffOverridesGlobalDefaults() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
		properties.getRetry().setMultiplier(3.0d);
		MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();
		config.setRetryBaseDelay(Duration.ofSeconds(2));
		config.setRetryMultiplier(4.0d);

		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(2));
		assertThat(MangoSwarmDaemon.retryDelay(2, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(8));
		assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(32));
	}

	@Test
	void retryBackoffIsCappedByMaxDelay() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getRetry().setBaseDelay(Duration.ofMinutes(10));
		properties.getRetry().setMultiplier(10.0d);
		properties.getRetry().setMaxDelay(Duration.ofMinutes(30));
		MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();

		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ofMinutes(10));
		assertThat(MangoSwarmDaemon.retryDelay(2, config, properties.getRetry()))
				.isEqualTo(Duration.ofMinutes(30));
		assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry()))
				.isEqualTo(Duration.ofMinutes(30));
	}

	@Test
	void retryBackoffHandlesZeroNegativeAndOverflowingValues() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();

		properties.getRetry().setBaseDelay(Duration.ofSeconds(-1));
		properties.getRetry().setMultiplier(0.5d);
		assertThat(MangoSwarmDaemon.retryDelay(5, config, properties.getRetry()))
				.isEqualTo(Duration.ZERO);

		properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
		properties.getRetry().setMaxDelay(Duration.ZERO);
		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ZERO);

		properties.getRetry().setBaseDelay(Duration.ofNanos(Long.MAX_VALUE));
		properties.getRetry().setMultiplier(Double.MAX_VALUE);
		properties.getRetry().setMaxDelay(Duration.ofHours(1));
		assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry()))
				.isEqualTo(Duration.ofHours(1));
	}

	@Test
	void retryBackoffUsesLegacyDelayAndTaskMaxDelayOverrides() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
		properties.getRetry().setMaxDelay(Duration.ofMinutes(5));
		MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();
		config.setRetryDelay(Duration.ofSeconds(7));
		config.setRetryMaxDelay(Duration.ofSeconds(10));

		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(7));
		assertThat(MangoSwarmDaemon.retryDelay(4, config, properties.getRetry()))
				.isEqualTo(Duration.ofSeconds(10));
	}

	@Test
	void retryBackoffHandlesNullAndNegativeMaxDelay() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
		MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();

		properties.getRetry().setMaxDelay(null);
		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ZERO);

		properties.getRetry().setMaxDelay(Duration.ofSeconds(-1));
		assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry()))
				.isEqualTo(Duration.ZERO);
	}

	@Test
	void maintenanceBatchSizeDefaultsAndClampsConfiguredValues() throws Exception {
		MangoSwarmProperties defaults = properties(10, 1, 1);
		defaults.setCleanup(null);
		MangoSwarmDaemon defaultDaemon = daemon(defaults, new FakeRepository(0), 1, new CompletingHandler());

		MangoSwarmProperties nonPositive = properties(10, 1, 1);
		nonPositive.getCleanup().setBatchSize(0);
		MangoSwarmDaemon clampedDaemon = daemon(nonPositive, new FakeRepository(0), 1, new CompletingHandler());

		assertThat(invokeInstance(defaultDaemon, "maintenanceBatchSize", new Class<?>[] {}))
				.isEqualTo(1_000);
		assertThat(invokeInstance(clampedDaemon, "maintenanceBatchSize", new Class<?>[] {}))
				.isEqualTo(1);
		defaultDaemon.stop();
		clampedDaemon.stop();
	}

	@Test
	void privateTimingHelpersHandleNullZeroAndPositiveDurations() {
		assertThat(MangoSwarmDaemon.emptyQueueBackoff(null, 1)).isEqualTo(Duration.ZERO);
		assertThat(MangoSwarmDaemon.emptyQueueBackoff(Duration.ofMillis(-1), 1)).isEqualTo(Duration.ZERO);
		assertThat(MangoSwarmDaemon.emptyQueueBackoff(Duration.ZERO, 1)).isEqualTo(Duration.ZERO);
		assertThat(MangoSwarmDaemon.emptyQueueBackoff(Duration.ofMillis(250), 1))
				.isEqualTo(Duration.ofMillis(250));
		assertThat(MangoSwarmDaemon.emptyQueueBackoff(Duration.ofSeconds(3), 2)).isEqualTo(Duration.ofSeconds(5));
		assertThat(MangoSwarmDaemon.emptyQueueBackoff(Duration.ofMillis(1), 20)).isEqualTo(Duration.ofSeconds(5));

		assertThat(MangoSwarmDaemon.minPositive(null, Duration.ZERO)).isNull();
		assertThat(MangoSwarmDaemon.minPositive(null, Duration.ofMillis(5))).isEqualTo(Duration.ofMillis(5));
		assertThat(MangoSwarmDaemon.minPositive(Duration.ofMillis(10), Duration.ZERO))
				.isEqualTo(Duration.ofMillis(10));
		assertThat(MangoSwarmDaemon.minPositive(Duration.ofMillis(10), Duration.ofMillis(5)))
				.isEqualTo(Duration.ofMillis(5));
		assertThat(MangoSwarmDaemon.minPositive(Duration.ofMillis(10), Duration.ofMillis(50)))
				.isEqualTo(Duration.ofMillis(10));

		assertThat(MangoSwarmDaemon.isPositive(null)).isFalse();
		assertThat(MangoSwarmDaemon.isPositive(Duration.ZERO)).isFalse();
		assertThat(MangoSwarmDaemon.isPositive(Duration.ofNanos(1))).isTrue();

		assertThatCode(() -> {
					MangoSwarmDaemon.sleepIfPositive(Duration.ZERO);
					MangoSwarmDaemon.sleepIfPositive(Duration.ofNanos(1));
				})
				.doesNotThrowAnyException();
	}

	private static MangoSwarmDaemon daemon(
			MangoSwarmProperties properties, FakeRepository repository, int activeWorkers, TaskHandler<?> handler) {
		return daemon(properties, repository, new TestWorkerRegistry(activeWorkers), handler);
	}

	private static MangoSwarmDaemon daemon(
			MangoSwarmProperties properties,
			FakeRepository repository,
			WorkerRegistry workers,
			TaskHandler<?> handler) {
		TaskHandlerRegistry registry = new TaskHandlerRegistry(
				List.of(handler), properties.getTaskTypes().keySet(), false);
		return new MangoSwarmDaemon(workers, repository, registry, properties);
	}

	private static MangoSwarmProperties properties(int rate, int concurrency, int batchSize) {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getExecutor().setMaxThreads("16");
		MangoSwarmProperties.TaskType email = new MangoSwarmProperties.TaskType();
		email.setRate(rate);
		email.setPeriod(Duration.ofSeconds(1));
		email.setConcurrency(concurrency);
		email.setTimeout(Duration.ofSeconds(30));
		email.setBatchSize(batchSize);
		email.setIdempotent(true);
		email.setReclaimOnTimeout(true);
		properties.getTaskTypes().put("email", email);
		return properties;
	}

	private static void awaitCounter(java.util.function.IntSupplier supplier, int expected)
			throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (supplier.getAsInt() < expected && System.nanoTime() < deadline) {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException();
			}
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
		}
	}

	private static Object invokeInstance(Object target, String name, Class<?>[] parameterTypes, Object... args)
			throws Exception {
		Method method = MangoSwarmDaemon.class.getDeclaredMethod(name, parameterTypes);
		method.setAccessible(true);
		return method.invoke(target, args);
	}

	private static Object fieldValue(Object target, String name) throws Exception {
		Field field = MangoSwarmDaemon.class.getDeclaredField(name);
		field.setAccessible(true);
		return field.get(target);
	}

	private static Object invokeAccessor(Object target, String name) throws Exception {
		Method method = target.getClass().getDeclaredMethod(name);
		method.setAccessible(true);
		return method.invoke(target);
	}

	private static Object newRuntimeProgressReporter(MangoSwarmDaemon daemon) throws Exception {
		Class<?> reporterType = Class.forName(MangoSwarmDaemon.class.getName() + "$RuntimeProgressReporter");
		var constructor = reporterType.getDeclaredConstructor(MangoSwarmDaemon.class, UUID.class, UUID.class);
		constructor.setAccessible(true);
		return constructor.newInstance(daemon, UUID.randomUUID(), UUID.randomUUID());
	}

	private static void invokeReport(Object reporter, String state, Integer percent, String message) throws Exception {
		Method method = reporter.getClass().getDeclaredMethod("report", String.class, Integer.class, String.class);
		method.setAccessible(true);
		method.invoke(reporter, state, percent, message);
	}

	private static TaskRecord taskRecord(String taskType, Instant now) {
		return new TaskRecord(
				UUID.nameUUIDFromBytes(("task-" + taskType).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
				taskType,
				JsonNodeFactory.instance.objectNode(),
				ie.bitstep.mango.swarm.TaskStatus.CLAIMED,
				now,
				UUID.randomUUID(),
				now,
				1,
				now,
				now);
	}

	@SwarmHandler("email")
	private static class CompletingHandler implements TaskHandler<String> {
		@Override
		public PayloadExtractor<String> payloadExtractor() {
			return reader -> "ok";
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) throws TaskHandlerException {
			context.progress(100);
			return TaskExecutionResult.completed();
		}
	}

	private static final class CountingCompletingHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private CountingCompletingHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			assertThat(context.payload()).isEqualTo("ok");
			executed.countDown();
			return TaskExecutionResult.completed();
		}
	}

	private static final class NullResultHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private NullResultHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public PayloadExtractor<String> payloadExtractor() {
			return reader -> "payload";
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			assertThat(context.payload()).isEqualTo("payload");
			executed.countDown();
			return null;
		}
	}

	private static final class BlockingHandler extends CompletingHandler {
		private final CountDownLatch entered;
		private final CountDownLatch release;

		private BlockingHandler(CountDownLatch entered, CountDownLatch release) {
			this.entered = entered;
			this.release = release;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) throws TaskHandlerException {
			entered.countDown();
			try {
				release.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new TaskHandlerException("Interrupted while waiting for test handler release", ex);
			}
			return TaskExecutionResult.completed();
		}
	}

	private static final class FailingHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private FailingHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			executed.countDown();
			return TaskExecutionResult.failed("temporary failure");
		}
	}

	private static final class ThrowingHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private ThrowingHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			executed.countDown();
			throw new IllegalStateException("boom");
		}
	}

	private static final class ProgressHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private ProgressHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			context.progress(25, "connecting");
			context.progress(100, "completed");
			executed.countDown();
			return TaskExecutionResult.completed();
		}
	}

	private static final class StableProgressHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private StableProgressHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			context.progress(25, "same");
			context.progress(30, "same");
			context.progress(34, "same");
			context.progress(35, "same");
			executed.countDown();
			return TaskExecutionResult.completed();
		}
	}

	private static final class DuplicateProgressHandler extends CompletingHandler {
		private final CountDownLatch executed;

		private DuplicateProgressHandler(CountDownLatch executed) {
			this.executed = executed;
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			context.progress(25, "same");
			context.progress(25, "same");
			executed.countDown();
			return TaskExecutionResult.completed();
		}
	}

	private static final class TestWorkerRegistry implements WorkerRegistry {
		private final int activeWorkers;
		private int heartbeatCalls;

		private TestWorkerRegistry(int activeWorkers) {
			this.activeWorkers = activeWorkers;
		}

		@Override
		public int heartbeat(UUID workerId, String hostname, Instant startedAt, Instant now) {
			heartbeatCalls++;
			return activeWorkers;
		}

		@Override
		public int countActiveWorkers(Instant now) {
			return activeWorkers;
		}
	}

	private static final class FakeRepository implements TaskRepository {
		private final AtomicInteger nextId = new AtomicInteger(1);
		private int available;
		private final List<Integer> claimLimits = new ArrayList<>();
		private final List<Instant> claimInstants = new ArrayList<>();
		private int lastClaimLimit;
		private int reclaimCalls;
		private int failTimedOutCalls;
		private int lastTimeoutRecoveryLimit;
		private int failedTaskCalls;
		private int completedTaskCalls;
		private int rescheduleAfterFailureCalls;
		private int progressCalls;
		private int markInProgressCalls;
		private int requeueCalls;
		private int lastProgressPercent;
		private String lastProgressDescription;
		private String lastFailureMessage;
		private String lastRequeueReason;
		private Instant retryAvailableAt;
		private int deleteCompletedCalls;
		private int deleteFailedCalls;
		private int lastDeleteLimit;
		private Runnable afterClaimLimitCalculated = () -> {};

		private FakeRepository(int available) {
			this.available = available;
		}

		private void setAvailable(int available) {
			this.available = available;
		}

		@Override
		public UUID queue(String taskType, com.fasterxml.jackson.databind.JsonNode payload, Instant availableAt) {
			return UUID.randomUUID();
		}

		@Override
		public List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit) {
			lastClaimLimit = limit;
			claimLimits.add(limit);
			claimInstants.add(now);
			afterClaimLimitCalculated.run();
			int count = Math.min(limit, available);
			available -= count;
			List<TaskRecord> records = new ArrayList<>();
			for (int i = 0; i < count; i++) {
				records.add(new TaskRecord(
						UUID.nameUUIDFromBytes(
								("task-" + nextId.getAndIncrement()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
						taskType,
						JsonNodeFactory.instance.objectNode(),
						ie.bitstep.mango.swarm.TaskStatus.CLAIMED,
						now,
						workerId,
						now,
						1,
						now,
						now));
			}
			return records;
		}

		@Override
		public void markInProgress(UUID taskId, UUID workerId, Instant now) {
			markInProgressCalls++;
		}

		@Override
		public void updateRuntime(
				UUID taskId,
				UUID workerId,
				Instant now,
				String executionState,
				Integer progressPercent,
				String message) {
			progressCalls++;
			lastProgressPercent = progressPercent == null ? -1 : progressPercent;
			lastProgressDescription = message;
		}

		@Override
		public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {
			progressCalls++;
			lastProgressPercent = progressPercent;
			lastProgressDescription = description;
		}

		@Override
		public void markCompleted(UUID taskId, UUID workerId, Instant now) {
			completedTaskCalls++;
		}

		@Override
		public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
			failedTaskCalls++;
			lastFailureMessage = errorMessage;
		}

		@Override
		public void rescheduleAfterFailure(
				UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage) {
			rescheduleAfterFailureCalls++;
			retryAvailableAt = availableAt;
		}

		@Override
		public void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason) {
			requeueCalls++;
			lastRequeueReason = reason;
		}

		@Override
		public int reclaimTimedOut(String taskType, Duration timeout, Instant now, int limit) {
			reclaimCalls++;
			lastTimeoutRecoveryLimit = limit;
			return 0;
		}

		@Override
		public int markTimedOutFailed(String taskType, Duration timeout, Instant now, int limit) {
			failTimedOutCalls++;
			lastTimeoutRecoveryLimit = limit;
			return 0;
		}

		@Override
		public int deleteCompletedOlderThan(Duration retention, Instant now, int limit) {
			deleteCompletedCalls++;
			lastDeleteLimit = limit;
			return 0;
		}

		@Override
		public int deleteFailedOlderThan(Duration retention, Instant now, int limit) {
			deleteFailedCalls++;
			lastDeleteLimit = limit;
			return 0;
		}
	}
}
