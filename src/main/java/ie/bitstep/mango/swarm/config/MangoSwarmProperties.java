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
	private boolean enabled = true;
	private boolean allowUnconfiguredHandlers = false;
	private Database database = new Database();
	private Worker worker = new Worker();
	private Cleanup cleanup = new Cleanup();
	private Executor executor = new Executor();
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
		private Duration pacerRetention = Duration.ofDays(30);
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

		public Duration getPacerRetention() {
			return pacerRetention;
		}

		public void setPacerRetention(Duration pacerRetention) {
			this.pacerRetention = pacerRetention;
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

	/** Reserved virtual-thread policy. Current Java 17 builds use platform threads. */
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
}
