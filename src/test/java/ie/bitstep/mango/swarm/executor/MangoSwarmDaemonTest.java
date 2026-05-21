package ie.bitstep.mango.swarm.executor;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRecord;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.handler.TaskHandler;
import ie.bitstep.mango.swarm.handler.TaskHandlerRegistry;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MangoSwarmDaemonTest {

    @Test
    void dividesRateByActiveWorkerCountForBatchAcquisition() {
        MangoSwarmProperties properties = properties(100, 50, 50);
        properties.getExecutor().setMaxThreads("100");
        FakeRepository repository = new FakeRepository(100);
        MangoSwarmDaemon daemon = daemon(properties, repository, 4, new CompletingHandler());
        Instant now = Instant.parse("2026-05-20T10:00:00Z");

        daemon.pollOnce(now);

        assertThat(repository.lastClaimLimit).isEqualTo(50);
        daemon.stop();
    }

    @Test
    void respectsConfiguredBatchAndConcurrencyAndExecutorCapacity() {
        MangoSwarmProperties properties = properties(100, 3, 20);
        properties.getExecutor().setMaxThreads("2");
        FakeRepository repository = new FakeRepository(100);
        MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

        daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

        assertThat(repository.lastClaimLimit).isEqualTo(2);
        daemon.stop();
    }

    @Test
    void immediatelyPollsAgainAfterNonEmptyClaim() {
        MangoSwarmProperties properties = properties(100, 5, 5);
        FakeRepository repository = new FakeRepository(100);
        MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

        Duration delay = daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

        assertThat(delay).isEqualTo(Duration.ZERO);
        assertThat(repository.lastClaimLimit).isEqualTo(5);
        daemon.stop();
    }

    @Test
    void reservesFutureSlotsAcrossTheBatchWindow() {
        MangoSwarmProperties properties = properties(1, 10, 5);
        properties.getTaskTypes().get("email").setPeriod(Duration.ofSeconds(3));
        FakeRepository repository = new FakeRepository(10);
        MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());
        Instant now = Instant.parse("2026-05-20T10:00:00Z");

        daemon.pollOnce(now);
        daemon.pollOnce(now.plusMillis(250));

        assertThat(repository.claimLimits).containsExactly(5, 5);
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
    void marksTimedOutFailedWhenReclaimDisabled() {
        MangoSwarmProperties properties = properties(10, 1, 1);
        properties.getTaskTypes().get("email").setReclaimOnTimeout(false);
        FakeRepository repository = new FakeRepository(0);
        MangoSwarmDaemon daemon = daemon(properties, repository, 1, new CompletingHandler());

        daemon.pollOnce(Instant.parse("2026-05-20T10:00:00Z"));

        assertThat(repository.failTimedOutCalls).isEqualTo(1);
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
        assertThat(repository.progressCalls).isEqualTo(2);
        assertThat(repository.lastProgressPercent).isEqualTo(100);
        assertThat(repository.lastProgressDescription).isEqualTo("completed");
        daemon.stop();
    }

    @Test
    void calculatesExponentialRetryDelayFromGlobalDefaults() {
        MangoSwarmProperties properties = new MangoSwarmProperties();
        properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
        properties.getRetry().setMultiplier(3.0d);
        MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();

        assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry())).isEqualTo(Duration.ofSeconds(5));
        assertThat(MangoSwarmDaemon.retryDelay(2, config, properties.getRetry())).isEqualTo(Duration.ofSeconds(15));
        assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry())).isEqualTo(Duration.ofSeconds(45));
    }

    @Test
    void taskRetryBackoffOverridesGlobalDefaults() {
        MangoSwarmProperties properties = new MangoSwarmProperties();
        properties.getRetry().setBaseDelay(Duration.ofSeconds(5));
        properties.getRetry().setMultiplier(3.0d);
        MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();
        config.setRetryBaseDelay(Duration.ofSeconds(2));
        config.setRetryMultiplier(4.0d);

        assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry())).isEqualTo(Duration.ofSeconds(2));
        assertThat(MangoSwarmDaemon.retryDelay(2, config, properties.getRetry())).isEqualTo(Duration.ofSeconds(8));
        assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry())).isEqualTo(Duration.ofSeconds(32));
    }

    @Test
    void retryBackoffIsCappedByMaxDelay() {
        MangoSwarmProperties properties = new MangoSwarmProperties();
        properties.getRetry().setBaseDelay(Duration.ofMinutes(10));
        properties.getRetry().setMultiplier(10.0d);
        properties.getRetry().setMaxDelay(Duration.ofMinutes(30));
        MangoSwarmProperties.TaskType config = new MangoSwarmProperties.TaskType();

        assertThat(MangoSwarmDaemon.retryDelay(1, config, properties.getRetry())).isEqualTo(Duration.ofMinutes(10));
        assertThat(MangoSwarmDaemon.retryDelay(2, config, properties.getRetry())).isEqualTo(Duration.ofMinutes(30));
        assertThat(MangoSwarmDaemon.retryDelay(3, config, properties.getRetry())).isEqualTo(Duration.ofMinutes(30));
    }

    private static MangoSwarmDaemon daemon(
            MangoSwarmProperties properties,
            FakeRepository repository,
            int activeWorkers,
            TaskHandler<?> handler) {
        WorkerRegistry workers = new WorkerRegistry() {
            @Override
            public int heartbeat(UUID workerId, String hostname, Instant startedAt, Instant now) {
                return activeWorkers;
            }

            @Override
            public int countActiveWorkers(Instant now) {
                return activeWorkers;
            }
        };
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(handler), properties.getTaskTypes().keySet(), false);
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

    private static class CompletingHandler implements TaskHandler<String> {
        @Override
        public String taskType() {
            return "email";
        }

        @Override
        public PayloadExtractor<String> payloadExtractor() {
            return reader -> "ok";
        }

        @Override
        public TaskExecutionResult execute(TaskExecutionContext<String> context) throws Exception {
            context.progress(100);
            return TaskExecutionResult.completed();
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
        public TaskExecutionResult execute(TaskExecutionContext<String> context) throws Exception {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
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

    private static final class FakeRepository implements TaskRepository {
        private final AtomicInteger nextId = new AtomicInteger(1);
        private final int available;
        private final List<Integer> claimLimits = new ArrayList<>();
        private int lastClaimLimit;
        private int reclaimCalls;
        private int failTimedOutCalls;
        private int failedTaskCalls;
        private int rescheduleAfterFailureCalls;
        private int progressCalls;
        private int lastProgressPercent;
        private String lastProgressDescription;
        private Instant retryAvailableAt;

        private FakeRepository(int available) {
            this.available = available;
        }

        @Override
        public UUID queue(String taskType, com.fasterxml.jackson.databind.JsonNode payload, Instant availableAt) {
            return UUID.randomUUID();
        }

        @Override
        public UUID queueInNextSlot(
                String taskType,
                com.fasterxml.jackson.databind.JsonNode payload,
                Instant requestedAt,
                Duration slotSpacing) {
            return UUID.randomUUID();
        }

        @Override
        public List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit) {
            lastClaimLimit = limit;
            claimLimits.add(limit);
            int count = Math.min(limit, available);
            List<TaskRecord> records = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                records.add(new TaskRecord(
                        UUID.nameUUIDFromBytes(("task-" + nextId.getAndIncrement()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                        taskType,
                        JsonNodeFactory.instance.objectNode(),
                        ie.bitstep.mango.swarm.TaskStatus.claimed,
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
        }

        @Override
        public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {
            progressCalls++;
            lastProgressPercent = progressPercent;
            lastProgressDescription = description;
        }

        @Override
        public void markCompleted(UUID taskId, UUID workerId, Instant now) {
        }

        @Override
        public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
            failedTaskCalls++;
        }

        @Override
        public void rescheduleAfterFailure(
                UUID taskId,
                UUID workerId,
                Instant now,
                Instant availableAt,
                String errorMessage) {
            rescheduleAfterFailureCalls++;
            retryAvailableAt = availableAt;
        }

        @Override
        public int reclaimTimedOut(String taskType, Duration timeout, Instant now) {
            reclaimCalls++;
            return 0;
        }

        @Override
        public int markTimedOutFailed(String taskType, Duration timeout, Instant now) {
            failTimedOutCalls++;
            return 0;
        }
    }
}
