package ie.bitstep.mango.swarm.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for mango-swarm.
 *
 * <p>Prefix: {@code mango4j.swarm}
 */
@ConfigurationProperties(prefix = "mango4j.swarm")
public class MangoSwarmProperties {
	private static final int MAX_SCHEMA_LENGTH = 63;
	private static final int MAX_TASK_TYPE_NAME_LENGTH = 128;
	private static final int MAX_EXECUTOR_THREADS = 256;
	private static final Duration DEFAULT_EXECUTOR_POLL_INTERVAL = Duration.ofMillis(100);
	private static final Duration MAX_EXECUTOR_POLL_INTERVAL = Duration.ofMinutes(1);
	private static final Duration DEFAULT_WORKER_HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
	private static final Duration MAX_WORKER_HEARTBEAT_INTERVAL = Duration.ofMinutes(1);
	private static final Duration DEFAULT_WORKER_STALE_AFTER = Duration.ofSeconds(30);
	private static final Duration MAX_WORKER_STALE_AFTER = Duration.ofHours(24);
	private static final Duration MAX_CLEANUP_INTERVAL = Duration.ofHours(24);
	private static final Duration MAX_CLEANUP_RETENTION = Duration.ofDays(365);
	private static final int MAX_CLEANUP_BATCH_SIZE = 1_000;
	private static final Duration DEFAULT_RUNTIME_MIN_UPDATE_INTERVAL = Duration.ofSeconds(30);
	private static final Duration MAX_RUNTIME_MIN_UPDATE_INTERVAL = Duration.ofHours(1);
	private static final int MAX_RUNTIME_PROGRESS_THRESHOLD_PERCENT = 100;
	private static final Duration DEFAULT_TASK_PERIOD = Duration.ofSeconds(1);
	private static final Duration MAX_TASK_PERIOD = Duration.ofHours(24);
	private static final int MAX_TASK_RATE = 1_000;
	private static final int MAX_TASK_CONCURRENCY = 256;
	private static final Duration DEFAULT_TASK_TIMEOUT = Duration.ofMinutes(1);
	private static final Duration MAX_TASK_TIMEOUT = Duration.ofHours(24);
	private static final int MAX_TASK_BATCH_SIZE = 1_000;
	private static final int MAX_TASK_MAX_ATTEMPTS = 100;
	private static final Duration MAX_RETRY_DELAY = Duration.ofHours(24);
	private static final double MAX_RETRY_MULTIPLIER = 10.0d;

	private boolean enabled = true;
	private boolean allowUnconfiguredHandlers = false;
	private Database database = new Database();
	private Worker worker = new Worker();
	private Cleanup cleanup = new Cleanup();
	private Executor executor = new Executor();
	private RuntimeState runtime = new RuntimeState();
	private Retry retry = new Retry();
	private Map<String, TaskType> taskTypes = new LinkedHashMap<>();

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public boolean isAllowUnconfiguredHandlers() {
		return allowUnconfiguredHandlers;
	}

	public void setAllowUnconfiguredHandlers(boolean allowUnconfiguredHandlers) {
		this.allowUnconfiguredHandlers = allowUnconfiguredHandlers;
	}

	public Database getDatabase() {
		return database;
	}

	public void setDatabase(Database database) {
		this.database = database;
	}

	public Worker getWorker() {
		return worker;
	}

	public void setWorker(Worker worker) {
		this.worker = worker;
	}

	public Executor getExecutor() {
		return executor;
	}

	public void setExecutor(Executor executor) {
		this.executor = executor;
	}

	public Cleanup getCleanup() {
		return cleanup;
	}

	public void setCleanup(Cleanup cleanup) {
		this.cleanup = cleanup;
	}

	public RuntimeState getRuntime() {
		return runtime;
	}

	public void setRuntime(RuntimeState runtime) {
		this.runtime = runtime;
	}

	public Retry getRetry() {
		return retry;
	}

	public void setRetry(Retry retry) {
		this.retry = retry;
	}

	public Map<String, TaskType> getTaskTypes() {
		return taskTypes;
	}

	public void setTaskTypes(Map<String, TaskType> taskTypes) {
		this.taskTypes = taskTypes;
	}

	/**
	 * Normalizes config values into bounded, runtime-safe forms.
	 *
	 * <p>This preserves existing disable-by-zero/null semantics where the runtime already relies on them, but it trims
	 * identifiers and caps values that can otherwise cause resource exhaustion or startup failures.
	 */
	public MangoSwarmProperties normalize() {
		if (database == null) {
			database = new Database();
		}
		database.setSchema(normalizeSchema(database.getSchema()));

		if (worker == null) {
			worker = new Worker();
		}
		worker.setHeartbeatInterval(normalizeRequiredDuration(
				worker.getHeartbeatInterval(), DEFAULT_WORKER_HEARTBEAT_INTERVAL, MAX_WORKER_HEARTBEAT_INTERVAL));
		Duration staleAfter =
				normalizeRequiredDuration(worker.getStaleAfter(), DEFAULT_WORKER_STALE_AFTER, MAX_WORKER_STALE_AFTER);
		worker.setStaleAfter(
				staleAfter.compareTo(worker.getHeartbeatInterval()) < 0 ? worker.getHeartbeatInterval() : staleAfter);

		if (executor == null) {
			executor = new Executor();
		}
		executor.setMaxThreads(normalizeExecutorMaxThreads(executor.getMaxThreads()));
		executor.setPollInterval(normalizeRequiredDuration(
				executor.getPollInterval(), DEFAULT_EXECUTOR_POLL_INTERVAL, MAX_EXECUTOR_POLL_INTERVAL));
		if (executor.getQueueStrategy() == null) {
			executor.setQueueStrategy(QueueStrategy.CALLER_RUNS);
		}
		if (executor.getVirtualThreads() == null) {
			executor.setVirtualThreads(VirtualThreads.AUTO);
		}

		if (cleanup != null) {
			cleanup.setInterval(capOptionalDuration(cleanup.getInterval(), MAX_CLEANUP_INTERVAL));
			cleanup.setCompletedRetention(capOptionalDuration(cleanup.getCompletedRetention(), MAX_CLEANUP_RETENTION));
			cleanup.setFailedRetention(capOptionalDuration(cleanup.getFailedRetention(), MAX_CLEANUP_RETENTION));
			cleanup.setBatchSize(clampInt(cleanup.getBatchSize(), 1, MAX_CLEANUP_BATCH_SIZE));
		}

		if (runtime == null) {
			runtime = new RuntimeState();
		}
		runtime.setProgressThresholdPercent(
				clampInt(runtime.getProgressThresholdPercent(), 0, MAX_RUNTIME_PROGRESS_THRESHOLD_PERCENT));
		runtime.setMinUpdateInterval(
				capOptionalDuration(runtime.getMinUpdateInterval(), MAX_RUNTIME_MIN_UPDATE_INTERVAL));

		if (retry == null) {
			retry = new Retry();
		}
		retry.setBaseDelay(capOptionalDuration(retry.getBaseDelay(), MAX_RETRY_DELAY));
		retry.setMultiplier(clampMultiplier(retry.getMultiplier()));
		retry.setMaxDelay(capOptionalDuration(retry.getMaxDelay(), MAX_RETRY_DELAY));

		taskTypes = normalizeTaskTypes(taskTypes);
		return this;
	}

	private static Map<String, TaskType> normalizeTaskTypes(Map<String, TaskType> taskTypes) {
		Map<String, TaskType> normalized = new LinkedHashMap<>();
		if (taskTypes == null || taskTypes.isEmpty()) {
			return normalized;
		}
		taskTypes.forEach((name, config) -> {
			String normalizedName = normalizeTaskTypeName(name);
			TaskType normalizedConfig = config == null ? new TaskType() : config;
			normalizedConfig.setRate(clampInt(normalizedConfig.getRate(), 0, MAX_TASK_RATE));
			normalizedConfig.setPeriod(
					normalizeRequiredDuration(normalizedConfig.getPeriod(), DEFAULT_TASK_PERIOD, MAX_TASK_PERIOD));
			normalizedConfig.setConcurrency(clampInt(normalizedConfig.getConcurrency(), 1, MAX_TASK_CONCURRENCY));
			normalizedConfig.setTimeout(
					normalizeRequiredDuration(normalizedConfig.getTimeout(), DEFAULT_TASK_TIMEOUT, MAX_TASK_TIMEOUT));
			if (normalizedConfig.getBatchSize() != null) {
				normalizedConfig.setBatchSize(clampInt(normalizedConfig.getBatchSize(), 1, MAX_TASK_BATCH_SIZE));
			}
			normalizedConfig.setMaxAttempts(clampInt(normalizedConfig.getMaxAttempts(), 1, MAX_TASK_MAX_ATTEMPTS));
			normalizedConfig.setRetryBaseDelay(
					capOptionalDuration(normalizedConfig.getRetryBaseDelay(), MAX_RETRY_DELAY));
			normalizedConfig.setRetryDelay(capOptionalDuration(normalizedConfig.getRetryDelay(), MAX_RETRY_DELAY));
			normalizedConfig.setRetryMaxDelay(
					capOptionalDuration(normalizedConfig.getRetryMaxDelay(), MAX_RETRY_DELAY));
			normalizedConfig.setRetryMultiplier(normalizeNullableMultiplier(normalizedConfig.getRetryMultiplier()));
			if (normalized.putIfAbsent(normalizedName, normalizedConfig) != null) {
				throw new IllegalArgumentException(
						"Duplicate mango4j.swarm.task-types key after normalization: " + normalizedName);
			}
		});
		return normalized;
	}

	private static String normalizeSchema(String schema) {
		if (schema == null) {
			return null;
		}
		String normalized = schema.trim();
		if (normalized.isEmpty()) {
			return null;
		}
		if (normalized.length() > MAX_SCHEMA_LENGTH) {
			throw new IllegalArgumentException("mango4j.swarm.database.schema is too long: " + schema);
		}
		return normalized;
	}

	private static String normalizeTaskTypeName(String taskType) {
		if (taskType == null) {
			throw new IllegalArgumentException("Task type name must not be null");
		}
		String normalized = taskType.trim();
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException("Task type name must not be blank");
		}
		if (normalized.length() > MAX_TASK_TYPE_NAME_LENGTH) {
			throw new IllegalArgumentException("Task type name is too long: " + taskType);
		}
		return normalized;
	}

	private static String normalizeExecutorMaxThreads(String maxThreads) {
		if (maxThreads == null) {
			return "auto";
		}
		String normalized = maxThreads.trim();
		if (normalized.isEmpty() || "auto".equalsIgnoreCase(normalized)) {
			return "auto";
		}
		try {
			return Integer.toString(clampInt(Integer.parseInt(normalized), 1, MAX_EXECUTOR_THREADS));
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(
					"mango4j.swarm.executor.maxThreads must be numeric or auto: " + maxThreads, ex);
		}
	}

	private static Duration normalizeRequiredDuration(Duration value, Duration defaultValue, Duration maxValue) {
		if (value == null || value.isZero() || value.isNegative()) {
			return defaultValue;
		}
		return value.compareTo(maxValue) > 0 ? maxValue : value;
	}

	private static Duration capOptionalDuration(Duration value, Duration maxValue) {
		if (value == null || value.isZero() || value.isNegative()) {
			return value;
		}
		return value.compareTo(maxValue) > 0 ? maxValue : value;
	}

	private static int clampInt(int value, int minValue, int maxValue) {
		return Math.max(minValue, Math.min(maxValue, value));
	}

	private static double clampMultiplier(double multiplier) {
		if (!Double.isFinite(multiplier) || multiplier < 1.0d) {
			return 1.0d;
		}
		return Math.min(multiplier, MAX_RETRY_MULTIPLIER);
	}

	private static Double normalizeNullableMultiplier(Double multiplier) {
		if (multiplier == null) {
			return null;
		}
		return clampMultiplier(multiplier);
	}

	/** Worker heartbeat and stale-worker pruning settings. */
	public static class Worker {
		private Duration heartbeatInterval = Duration.ofSeconds(10);
		private Duration staleAfter = Duration.ofSeconds(30);

		public Duration getHeartbeatInterval() {
			return heartbeatInterval;
		}

		public void setHeartbeatInterval(Duration heartbeatInterval) {
			this.heartbeatInterval = heartbeatInterval;
		}

		public Duration getStaleAfter() {
			return staleAfter;
		}

		public void setStaleAfter(Duration staleAfter) {
			this.staleAfter = staleAfter;
		}
	}

	/** Periodic cleanup retention and cadence settings. */
	public static class Cleanup {
		private boolean enabled = true;
		private Duration interval = Duration.ofMinutes(10);
		private Duration completedRetention = Duration.ofDays(30);
		private Duration failedRetention = Duration.ofDays(90);
		private int batchSize = 1_000;

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public Duration getInterval() {
			return interval;
		}

		public void setInterval(Duration interval) {
			this.interval = interval;
		}

		public Duration getCompletedRetention() {
			return completedRetention;
		}

		public void setCompletedRetention(Duration completedRetention) {
			this.completedRetention = completedRetention;
		}

		public Duration getFailedRetention() {
			return failedRetention;
		}

		public void setFailedRetention(Duration failedRetention) {
			this.failedRetention = failedRetention;
		}

		public int getBatchSize() {
			return batchSize;
		}

		public void setBatchSize(int batchSize) {
			this.batchSize = batchSize;
		}
	}

	/** Database schema and native-SQL schema-prefix settings. */
	public static class Database {
		private String schema;
		private boolean applySchemaToHibernateDefault = true;

		public String getSchema() {
			return schema;
		}

		public void setSchema(String schema) {
			this.schema = schema;
		}

		public boolean isApplySchemaToHibernateDefault() {
			return applySchemaToHibernateDefault;
		}

		public void setApplySchemaToHibernateDefault(boolean applySchemaToHibernateDefault) {
			this.applySchemaToHibernateDefault = applySchemaToHibernateDefault;
		}
	}

	/** Runtime state persistence throttling. */
	public static class RuntimeState {
		private int progressThresholdPercent = 10;
		private Duration minUpdateInterval = Duration.ofSeconds(30);

		public int getProgressThresholdPercent() {
			return progressThresholdPercent;
		}

		public void setProgressThresholdPercent(int progressThresholdPercent) {
			this.progressThresholdPercent = progressThresholdPercent;
		}

		public Duration getMinUpdateInterval() {
			return minUpdateInterval;
		}

		public void setMinUpdateInterval(Duration minUpdateInterval) {
			this.minUpdateInterval = minUpdateInterval;
		}
	}

	/** Local executor and polling settings. */
	public static class Executor {
		private String maxThreads = "auto";
		private Duration pollInterval = Duration.ofMillis(100);
		private QueueStrategy queueStrategy = QueueStrategy.CALLER_RUNS;
		private VirtualThreads virtualThreads = VirtualThreads.AUTO;

		public String getMaxThreads() {
			return maxThreads;
		}

		public void setMaxThreads(String maxThreads) {
			this.maxThreads = maxThreads;
		}

		public Duration getPollInterval() {
			return pollInterval;
		}

		public void setPollInterval(Duration pollInterval) {
			this.pollInterval = pollInterval;
		}

		public QueueStrategy getQueueStrategy() {
			return queueStrategy;
		}

		public void setQueueStrategy(QueueStrategy queueStrategy) {
			this.queueStrategy = queueStrategy;
		}

		public VirtualThreads getVirtualThreads() {
			return virtualThreads;
		}

		public void setVirtualThreads(VirtualThreads virtualThreads) {
			this.virtualThreads = virtualThreads;
		}
	}

	/** Executor queue overload strategy. */
	public enum QueueStrategy {
		CALLER_RUNS,
		ABORT
	}

	/**
	 * Virtual-thread policy for the task executor.
	 *
	 * <p>{@link #ENABLED} and {@link #AUTO} both use virtual threads when running on Java 21+ and fall back to platform
	 * threads silently on earlier JVMs. {@link #DISABLED} always uses platform threads regardless of JVM version.
	 */
	public enum VirtualThreads {
		ENABLED,
		DISABLED,
		AUTO
	}

	/** Global retry backoff defaults. */
	public static class Retry {
		private Duration baseDelay = Duration.ZERO;
		private double multiplier = 2.0d;
		private Duration maxDelay = Duration.ofMinutes(30);

		public Duration getBaseDelay() {
			return baseDelay;
		}

		public void setBaseDelay(Duration baseDelay) {
			this.baseDelay = baseDelay;
		}

		public double getMultiplier() {
			return multiplier;
		}

		public void setMultiplier(double multiplier) {
			this.multiplier = multiplier;
		}

		public Duration getMaxDelay() {
			return maxDelay;
		}

		public void setMaxDelay(Duration maxDelay) {
			this.maxDelay = maxDelay;
		}
	}

	/** Per-task-type execution policy. */
	public static class TaskType {
		private TaskMode mode = TaskMode.EXECUTE;
		private int rate;
		private Duration period = Duration.ofSeconds(1);
		private int concurrency = 1;
		private Duration timeout = Duration.ofMinutes(1);
		private boolean reclaimOnTimeout;
		private boolean idempotent;
		private Integer batchSize;
		private int maxAttempts = 1;
		private Duration retryBaseDelay;
		private Double retryMultiplier;
		private Duration retryMaxDelay;
		private Duration retryDelay;

		public TaskMode getMode() {
			return mode;
		}

		public void setMode(TaskMode mode) {
			this.mode = mode;
		}

		public int getRate() {
			return rate;
		}

		public void setRate(int rate) {
			this.rate = rate;
		}

		public Duration getPeriod() {
			return period;
		}

		public void setPeriod(Duration period) {
			this.period = period;
		}

		public int getConcurrency() {
			return concurrency;
		}

		public void setConcurrency(int concurrency) {
			this.concurrency = concurrency;
		}

		public Duration getTimeout() {
			return timeout;
		}

		public void setTimeout(Duration timeout) {
			this.timeout = timeout;
		}

		public boolean isReclaimOnTimeout() {
			return reclaimOnTimeout;
		}

		public void setReclaimOnTimeout(boolean reclaimOnTimeout) {
			this.reclaimOnTimeout = reclaimOnTimeout;
		}

		public boolean isIdempotent() {
			return idempotent;
		}

		public void setIdempotent(boolean idempotent) {
			this.idempotent = idempotent;
		}

		public Integer getBatchSize() {
			return batchSize;
		}

		public void setBatchSize(Integer batchSize) {
			this.batchSize = batchSize;
		}

		public int getMaxAttempts() {
			return maxAttempts;
		}

		public void setMaxAttempts(int maxAttempts) {
			this.maxAttempts = maxAttempts;
		}

		public Duration getRetryBaseDelay() {
			return retryBaseDelay;
		}

		public void setRetryBaseDelay(Duration retryBaseDelay) {
			this.retryBaseDelay = retryBaseDelay;
		}

		public Double getRetryMultiplier() {
			return retryMultiplier;
		}

		public void setRetryMultiplier(Double retryMultiplier) {
			this.retryMultiplier = retryMultiplier;
		}

		public Duration getRetryMaxDelay() {
			return retryMaxDelay;
		}

		public void setRetryMaxDelay(Duration retryMaxDelay) {
			this.retryMaxDelay = retryMaxDelay;
		}

		/** Backward-compatible alias for the first retry delay. */
		public Duration getRetryDelay() {
			return retryDelay;
		}

		public void setRetryDelay(Duration retryDelay) {
			this.retryDelay = retryDelay;
		}
	}

	/** Per-task-type queueing and execution mode. */
	public enum TaskMode {
		EXECUTE,
		QUEUE,
		REJECT,
		DROP
	}
}
