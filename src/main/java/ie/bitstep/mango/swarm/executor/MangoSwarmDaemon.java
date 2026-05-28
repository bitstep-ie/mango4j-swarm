package ie.bitstep.mango.swarm.executor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRecord;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.handler.TaskHandler;
import ie.bitstep.mango.swarm.handler.TaskHandlerRegistry;
import ie.bitstep.mango.swarm.payload.PayloadExtractionException;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;
import ie.bitstep.mango.swarm.payload.PayloadReader;
import ie.bitstep.mango.swarm.rate.SmoothRateLimiter;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;

/**
 * Core swarm runtime loop.
 *
 * <p>Coordinates worker heartbeat, rate recalculation, task claiming, dispatch to executor threads, timeout handling,
 * retry scheduling, and retention cleanup.
 */
public class MangoSwarmDaemon {
	private static final Logger log = LoggerFactory.getLogger(MangoSwarmDaemon.class);
	private static final Duration MAX_EMPTY_QUEUE_BACKOFF = Duration.ofSeconds(5);
	private static final Duration TIMEOUT_RECOVERY_INTERVAL = Duration.ofSeconds(1);

	private final WorkerRegistry workerRegistry;
	private final TaskRepository taskRepository;
	private final TaskHandlerRegistry handlerRegistry;
	private final MangoSwarmProperties properties;
	private final UUID workerId = UUID.randomUUID();
	private final Instant startedAt = Instant.now();
	private final String hostname = resolveHostname();
	private final Map<String, SmoothRateLimiter> rateLimiters = new LinkedHashMap<>();
	private final TaskConcurrencyTracker taskConcurrencyTracker;
	private final ExecutorService executorService;
	private final Semaphore executorCapacity;
	private final AtomicBoolean running = new AtomicBoolean(false);
	private Thread daemonThread;
	private volatile int activeWorkers = 1;
	private volatile Instant lastHeartbeat = Instant.EPOCH;
	private volatile Instant lastCleanup = Instant.EPOCH;
	private volatile Instant lastTimeoutRecovery = Instant.EPOCH;
	private int consecutiveEmptyPolls = 0;

	public MangoSwarmDaemon(
			WorkerRegistry workerRegistry,
			TaskRepository taskRepository,
			TaskHandlerRegistry handlerRegistry,
			MangoSwarmProperties properties) {
		this.workerRegistry = workerRegistry;
		this.taskRepository = taskRepository;
		this.handlerRegistry = handlerRegistry;
		this.properties = properties;
		this.executorService = ExecutorFactory.create(properties.getExecutor());
		boolean virtual = properties.getExecutor().getVirtualThreads() != MangoSwarmProperties.VirtualThreads.DISABLED
				&& ExecutorFactory.virtualThreadsAvailable();
		int maxThreads =
				ExecutorFactory.resolveMaxThreads(properties.getExecutor().getMaxThreads(), virtual);
		this.executorCapacity = new Semaphore(maxThreads);
		Map<String, Integer> limits = new LinkedHashMap<>();
		properties.getTaskTypes().forEach((type, config) -> limits.put(type, Math.max(1, config.getConcurrency())));
		this.taskConcurrencyTracker = new TaskConcurrencyTracker(limits);
		properties.getTaskTypes().keySet().forEach(type -> rateLimiters.put(type, new SmoothRateLimiter()));
	}

	public void start() {
		if (!running.compareAndSet(false, true)) {
			return;
		}
		daemonThread = new Thread(this::runLoop, "mango-swarm-daemon");
		daemonThread.start();
	}

	public void stop() {
		running.set(false);
		if (daemonThread != null) {
			daemonThread.interrupt();
		}
		executorService.shutdown();
		try {
			executorService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	Duration pollOnce(Instant now) {
		heartbeatIfNeeded(now);
		cleanupIfNeeded(now);
		reclaimTimedOut(now);
		Duration nextRateLimitedPoll = null;
		boolean attemptedAnyClaim = false;
		boolean claimedAnyTasks = false;
		boolean dispatchedAnyTasks = false;
		for (Map.Entry<String, MangoSwarmProperties.TaskType> entry :
				properties.getTaskTypes().entrySet()) {
			String taskType = entry.getKey();
			MangoSwarmProperties.TaskType config = entry.getValue();
			double effectiveRate = Math.max(0.0d, (double) config.getRate() / Math.max(activeWorkers, 1));
			SmoothRateLimiter limiter = rateLimiters.get(taskType);
			limiter.configure(effectiveRate, config.getPeriod(), now);
			BatchDecision decision = decideBatchSize(taskType, config, effectiveRate, now);
			if (decision.claimLimit() <= 0) {
				if (decision.rateCapacity() <= 0 && isPositive(decision.nextExecutionDelay())) {
					nextRateLimitedPoll = minPositive(nextRateLimitedPoll, decision.nextExecutionDelay());
				}
				continue;
			}
			log.debug(
					"swarm claiming batch: taskType={}, workerId={}, activeWorkers={}, effectiveRate={}/{}, configuredBatch={}, claimLimit={}, rateCapacity={}, typeCapacity={}, executorCapacity={}",
					taskType,
					workerId,
					activeWorkers,
					effectiveRate,
					config.getPeriod(),
					decision.configuredBatch(),
					decision.claimLimit(),
					decision.rateCapacity(),
					decision.remainingTypeCapacity(),
					decision.remainingExecutorCapacity());
			attemptedAnyClaim = true;
			List<TaskRecord> claimed = taskRepository.claimBatch(taskType, workerId, now, decision.claimLimit());
			log.debug(
					"swarm claimed batch: taskType={}, workerId={}, requested={}, claimed={}, taskIds={}",
					taskType,
					workerId,
					decision.claimLimit(),
					claimed.size(),
					log.isDebugEnabled() ? claimed.stream().map(TaskRecord::id).toList() : List.of());
			if (!claimed.isEmpty()) {
				claimedAnyTasks = true;
			}
			for (TaskRecord task : claimed) {
				if (dispatch(task, now)) {
					dispatchedAnyTasks = true;
				}
			}
		}
		if (dispatchedAnyTasks) {
			consecutiveEmptyPolls = 0;
			return Duration.ZERO;
		}
		if (claimedAnyTasks) {
			consecutiveEmptyPolls = 0;
			return properties.getExecutor().getPollInterval();
		}
		if (attemptedAnyClaim) {
			consecutiveEmptyPolls++;
			Duration backoff = emptyQueueBackoff(properties.getExecutor().getPollInterval(), consecutiveEmptyPolls);
			log.debug(
					"swarm empty-queue backoff: workerId={}, consecutiveEmptyPolls={}, base={}, sleep={}",
					workerId,
					consecutiveEmptyPolls,
					properties.getExecutor().getPollInterval(),
					backoff);
			return backoff;
		}
		if (nextRateLimitedPoll != null) {
			return nextRateLimitedPoll;
		}
		return properties.getExecutor().getPollInterval();
	}

	static Duration emptyQueueBackoff(Duration base, int consecutiveEmptyPolls) {
		if (base == null || base.isNegative() || base.isZero()) {
			return Duration.ZERO;
		}
		Duration delay = base;
		int steps = Math.max(0, consecutiveEmptyPolls - 1);
		for (int i = 0; i < steps; i++) {
			if (delay.compareTo(MAX_EMPTY_QUEUE_BACKOFF) >= 0) {
				return MAX_EMPTY_QUEUE_BACKOFF;
			}
			delay = delay.multipliedBy(2);
			if (delay.compareTo(MAX_EMPTY_QUEUE_BACKOFF) >= 0) {
				return MAX_EMPTY_QUEUE_BACKOFF;
			}
		}
		return delay;
	}

	int calculateBatchSize(String taskType, MangoSwarmProperties.TaskType config, double effectiveRate, Instant now) {
		return decideBatchSize(taskType, config, effectiveRate, now).claimLimit();
	}

	private BatchDecision decideBatchSize(
			String taskType, MangoSwarmProperties.TaskType config, double effectiveRate, Instant now) {
		int remainingTypeCapacity = taskConcurrencyTracker.remaining(taskType);
		int remainingExecutorCapacity = executorCapacity.availablePermits();
		int configuredBatch = configuredBatch(config, effectiveRate);
		SmoothRateLimiter rateLimiter = rateLimiters.get(taskType);
		int rateCapacity = rateLimiter.permitsAvailable(now, configuredBatch);
		int claimLimit =
				Math.max(0, min(configuredBatch, rateCapacity, remainingTypeCapacity, remainingExecutorCapacity));
		return new BatchDecision(
				configuredBatch,
				rateCapacity,
				remainingTypeCapacity,
				remainingExecutorCapacity,
				claimLimit,
				rateLimiter.timeUntilNextPermit(now));
	}

	private void runLoop() {
		while (running.get()) {
			try {
				sleepIfPositive(pollOnce(Instant.now()));
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return;
			} catch (Exception ex) {
				log.warn("Mango swarm polling failed", ex);
			}
		}
	}

	static Duration minPositive(Duration current, Duration candidate) {
		if (!isPositive(candidate)) {
			return current;
		}
		if (current == null || candidate.compareTo(current) < 0) {
			return candidate;
		}
		return current;
	}

	static void sleepIfPositive(Duration delay) throws InterruptedException {
		if (!isPositive(delay)) {
			return;
		}
		long millis = delay.toMillis();
		int nanos = (int) delay.minusMillis(millis).toNanos();
		Thread.sleep(millis, nanos);
	}

	static boolean isPositive(Duration delay) {
		return delay != null && delay.compareTo(Duration.ZERO) > 0;
	}

	private void heartbeatIfNeeded(Instant now) {
		if (lastHeartbeat.plus(properties.getWorker().getHeartbeatInterval()).isAfter(now)) {
			return;
		}
		activeWorkers = workerRegistry.heartbeat(workerId, hostname, startedAt, now);
		lastHeartbeat = now;
	}

	private void cleanupIfNeeded(Instant now) {
		MangoSwarmProperties.Cleanup cleanup = properties.getCleanup();
		if (cleanup == null || !cleanup.isEnabled()) {
			return;
		}
		Duration interval = cleanup.getInterval();
		if (interval == null || interval.isNegative() || interval.isZero()) {
			return;
		}
		if (lastCleanup.plus(interval).isAfter(now)) {
			return;
		}
		Duration completedRetention = cleanup.getCompletedRetention();
		Duration failedRetention = cleanup.getFailedRetention();
		Duration pacerRetention = cleanup.getPacerRetention();
		int batchSize = maintenanceBatchSize();
		if (isRetentionEnabled(completedRetention)) {
			int deletedCompleted = taskRepository.deleteCompletedOlderThan(completedRetention, now, batchSize);
			log.debug(
					"swarm cleanup completed-tasks: workerId={}, retention={}, deleted={}",
					workerId,
					completedRetention,
					deletedCompleted);
		}
		if (isRetentionEnabled(failedRetention)) {
			int deletedFailed = taskRepository.deleteFailedOlderThan(failedRetention, now, batchSize);
			log.debug(
					"swarm cleanup failed-tasks: workerId={}, retention={}, deleted={}",
					workerId,
					failedRetention,
					deletedFailed);
		}
		if (isRetentionEnabled(pacerRetention)) {
			int deletedPacers = taskRepository.deleteTaskPacersOlderThan(pacerRetention, now, batchSize);
			log.debug(
					"swarm cleanup task-pacers: workerId={}, retention={}, deleted={}",
					workerId,
					pacerRetention,
					deletedPacers);
		}
		lastCleanup = now;
	}

	private static int configuredBatch(MangoSwarmProperties.TaskType config, double effectiveRate) {
		if (config.getBatchSize() != null) {
			return config.getBatchSize();
		}
		return Math.max(1, Math.min(config.getConcurrency(), (int) Math.ceil(effectiveRate)));
	}

	private void reclaimTimedOut(Instant now) {
		if (lastTimeoutRecovery.plus(TIMEOUT_RECOVERY_INTERVAL).isAfter(now)) {
			return;
		}
		int batchSize = maintenanceBatchSize();
		properties.getTaskTypes().forEach((type, config) -> {
			if (config.isReclaimOnTimeout() && config.isIdempotent()) {
				taskRepository.reclaimTimedOut(type, config.getTimeout(), now, batchSize);
			} else if (!config.isReclaimOnTimeout()) {
				taskRepository.markTimedOutFailed(type, config.getTimeout(), now, batchSize);
			}
		});
		lastTimeoutRecovery = now;
	}

	private int maintenanceBatchSize() {
		MangoSwarmProperties.Cleanup cleanup = properties.getCleanup();
		if (cleanup == null) {
			return 1_000;
		}
		return Math.max(1, cleanup.getBatchSize());
	}

	private static boolean isRetentionEnabled(Duration retention) {
		return retention != null && !retention.isNegative() && !retention.isZero();
	}

	private boolean dispatch(TaskRecord task, Instant now) {
		if (!taskConcurrencyTracker.tryAcquire(task.taskType())) {
			taskRepository.requeueClaimed(
					task.id(), workerId, now, now, "Local task-type concurrency unavailable; returned to queue");
			log.debug(
					"swarm dispatch deferred: taskType={}, taskId={}, workerId={}, reason=task-type-concurrency",
					task.taskType(),
					task.id(),
					workerId);
			return false;
		}
		if (!executorCapacity.tryAcquire()) {
			taskConcurrencyTracker.release(task.taskType());
			taskRepository.requeueClaimed(
					task.id(), workerId, now, now, "Local executor capacity unavailable; returned to queue");
			log.debug(
					"swarm dispatch deferred: taskType={}, taskId={}, workerId={}, reason=executor-capacity",
					task.taskType(),
					task.id(),
					workerId);
			return false;
		}
		log.debug(
				"swarm dispatch submitted: taskType={}, taskId={}, workerId={}, attempt={}",
				task.taskType(),
				task.id(),
				workerId,
				task.attemptCount());
		executorService.submit(() -> {
			try {
				taskRepository.markInProgress(task.id(), workerId, Instant.now());
				executeTask(task);
			} finally {
				taskConcurrencyTracker.release(task.taskType());
				executorCapacity.release();
			}
		});
		return true;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private void executeTask(TaskRecord task) {
		TaskHandler handler = handlerRegistry.get(task.taskType());
		try {
			log.debug(
					"swarm task execution started: taskType={}, taskId={}, workerId={}, attempt={}",
					task.taskType(),
					task.id(),
					workerId,
					task.attemptCount());
			Object payload = extractPayload(task.payload(), handler.payloadExtractor());
			TaskExecutionContext context = new TaskExecutionContext(
					task.id(),
					task.taskType(),
					workerId,
					task.attemptCount(),
					task.claimedAt(),
					payload,
					(percent, description) ->
							taskRepository.recordProgress(task.id(), workerId, Instant.now(), percent, description));
			TaskExecutionResult result = handler.execute(context);
			if (result == null || result.isCompleted()) {
				taskRepository.markCompleted(task.id(), workerId, Instant.now());
				log.debug(
						"swarm task execution completed: taskType={}, taskId={}, workerId={}, attempt={}",
						task.taskType(),
						task.id(),
						workerId,
						task.attemptCount());
			} else {
				handleFailedTask(task, result.message(), null);
				log.debug(
						"swarm task execution failed-result: taskType={}, taskId={}, workerId={}, attempt={}, message={}",
						task.taskType(),
						task.id(),
						workerId,
						task.attemptCount(),
						result.message());
			}
		} catch (Exception ex) {
			handleFailedTask(task, ex.getMessage(), ex);
			log.debug(
					"swarm task execution failed-exception: taskType={}, taskId={}, workerId={}, attempt={}",
					task.taskType(),
					task.id(),
					workerId,
					task.attemptCount(),
					ex);
		}
	}

	private void handleFailedTask(TaskRecord task, String errorMessage, Exception exception) {
		Instant now = Instant.now();
		MangoSwarmProperties.TaskType config = properties.getTaskTypes().get(task.taskType());
		int maxAttempts = Math.max(1, config.getMaxAttempts());
		if (task.attemptCount() < maxAttempts) {
			Duration retryDelay = retryDelay(task.attemptCount(), config, properties.getRetry());
			Instant retryAt = now.plus(retryDelay);
			taskRepository.rescheduleAfterFailure(task.id(), workerId, now, retryAt, errorMessage);
			log.debug(
					"swarm task scheduled for retry: taskType={}, taskId={}, workerId={}, attempt={}, maxAttempts={}, retryDelay={}, retryAt={}, error={}",
					task.taskType(),
					task.id(),
					workerId,
					task.attemptCount(),
					maxAttempts,
					retryDelay,
					retryAt,
					errorMessage,
					exception);
			return;
		}
		taskRepository.markFailed(task.id(), workerId, now, errorMessage);
	}

	static Duration retryDelay(
			int failedAttempt, MangoSwarmProperties.TaskType config, MangoSwarmProperties.Retry defaults) {
		Duration baseDelay = firstNonNull(config.getRetryBaseDelay(), config.getRetryDelay(), defaults.getBaseDelay());
		if (baseDelay == null || baseDelay.isNegative()) {
			baseDelay = Duration.ZERO;
		}
		double multiplier =
				config.getRetryMultiplier() == null ? defaults.getMultiplier() : config.getRetryMultiplier();
		multiplier = Math.max(1.0d, multiplier);
		int exponent = Math.max(0, failedAttempt - 1);
		double factor = Math.pow(multiplier, exponent);
		if (factor >= Long.MAX_VALUE) {
			return retryMaxDelay(config, defaults);
		}
		long baseNanos;
		try {
			baseNanos = baseDelay.toNanos();
		} catch (ArithmeticException ex) {
			return retryMaxDelay(config, defaults);
		}
		double nanos = baseNanos * factor;
		if (nanos >= Long.MAX_VALUE) {
			return retryMaxDelay(config, defaults);
		}
		Duration calculated = Duration.ofNanos(Math.max(0L, (long) nanos));
		Duration maxDelay = retryMaxDelay(config, defaults);
		if (maxDelay.isZero()) {
			return Duration.ZERO;
		}
		return calculated.compareTo(maxDelay) > 0 ? maxDelay : calculated;
	}

	private static Duration retryMaxDelay(MangoSwarmProperties.TaskType config, MangoSwarmProperties.Retry defaults) {
		Duration maxDelay = firstNonNull(config.getRetryMaxDelay(), defaults.getMaxDelay());
		if (maxDelay == null || maxDelay.isNegative()) {
			return Duration.ZERO;
		}
		return maxDelay;
	}

	@SafeVarargs
	private static <T> T firstNonNull(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	private <T> T extractPayload(JsonNode payload, PayloadExtractor<T> extractor) throws PayloadExtractionException {
		return extractor.extract(new PayloadReader(payload));
	}

	private static int min(int first, int... values) {
		int result = first;
		for (int value : values) {
			result = Math.min(result, value);
		}
		return result;
	}

	private static String resolveHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			return System.getenv().getOrDefault("HOSTNAME", "unknown");
		}
	}

	private static final class BatchDecision {
		private final int configuredBatch;
		private final int rateCapacity;
		private final int remainingTypeCapacity;
		private final int remainingExecutorCapacity;
		private final int claimLimit;
		private final Duration nextExecutionDelay;

		private BatchDecision(
				int configuredBatch,
				int rateCapacity,
				int remainingTypeCapacity,
				int remainingExecutorCapacity,
				int claimLimit,
				Duration nextExecutionDelay) {
			this.configuredBatch = configuredBatch;
			this.rateCapacity = rateCapacity;
			this.remainingTypeCapacity = remainingTypeCapacity;
			this.remainingExecutorCapacity = remainingExecutorCapacity;
			this.claimLimit = claimLimit;
			this.nextExecutionDelay = nextExecutionDelay;
		}

		private int configuredBatch() {
			return configuredBatch;
		}

		private int rateCapacity() {
			return rateCapacity;
		}

		private int remainingTypeCapacity() {
			return remainingTypeCapacity;
		}

		private int remainingExecutorCapacity() {
			return remainingExecutorCapacity;
		}

		private int claimLimit() {
			return claimLimit;
		}

		private Duration nextExecutionDelay() {
			return nextExecutionDelay;
		}
	}
}
