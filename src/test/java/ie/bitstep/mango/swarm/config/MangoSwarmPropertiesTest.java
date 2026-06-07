package ie.bitstep.mango.swarm.config;

import java.time.Duration;
import java.util.LinkedHashMap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MangoSwarmPropertiesTest {

	@Test
	void exposesDocumentedDefaults() {
		MangoSwarmProperties properties = new MangoSwarmProperties();

		assertThat(properties.isEnabled()).isTrue();
		assertThat(properties.isAllowUnconfiguredHandlers()).isFalse();
		assertThat(properties.getDatabase().getSchema()).isNull();
		assertThat(properties.getDatabase().isApplySchemaToHibernateDefault()).isTrue();
		assertThat(properties.getWorker().getHeartbeatInterval()).isEqualTo(Duration.ofSeconds(10));
		assertThat(properties.getWorker().getStaleAfter()).isEqualTo(Duration.ofSeconds(30));
		assertThat(properties.getCleanup().isEnabled()).isTrue();
		assertThat(properties.getCleanup().getInterval()).isEqualTo(Duration.ofMinutes(10));
		assertThat(properties.getCleanup().getCompletedRetention()).isEqualTo(Duration.ofDays(30));
		assertThat(properties.getCleanup().getFailedRetention()).isEqualTo(Duration.ofDays(90));
		assertThat(properties.getCleanup().getBatchSize()).isEqualTo(1_000);
		assertThat(properties.getExecutor().getMaxThreads()).isEqualTo("auto");
		assertThat(properties.getExecutor().getPollInterval()).isEqualTo(Duration.ofMillis(100));
		assertThat(properties.getExecutor().getQueueStrategy())
				.isEqualTo(MangoSwarmProperties.QueueStrategy.CALLER_RUNS);
		assertThat(properties.getExecutor().getVirtualThreads()).isEqualTo(MangoSwarmProperties.VirtualThreads.AUTO);
		assertThat(properties.getRetry().getBaseDelay()).isEqualTo(Duration.ZERO);
		assertThat(properties.getRetry().getMultiplier()).isEqualTo(2.0d);
		assertThat(properties.getRetry().getMaxDelay()).isEqualTo(Duration.ofMinutes(30));
		assertThat(properties.getTaskTypes()).isEmpty();
	}

	@Test
	void rootSettersReplaceNestedConfigurationObjects() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		MangoSwarmProperties.Database database = new MangoSwarmProperties.Database();
		MangoSwarmProperties.Worker worker = new MangoSwarmProperties.Worker();
		MangoSwarmProperties.Cleanup cleanup = new MangoSwarmProperties.Cleanup();
		MangoSwarmProperties.Executor executor = new MangoSwarmProperties.Executor();
		MangoSwarmProperties.Retry retry = new MangoSwarmProperties.Retry();
		LinkedHashMap<String, MangoSwarmProperties.TaskType> taskTypes = new LinkedHashMap<>();

		properties.setEnabled(false);
		properties.setAllowUnconfiguredHandlers(true);
		properties.setDatabase(database);
		properties.setWorker(worker);
		properties.setCleanup(cleanup);
		properties.setExecutor(executor);
		properties.setRetry(retry);
		properties.setTaskTypes(taskTypes);

		assertThat(properties.isEnabled()).isFalse();
		assertThat(properties.isAllowUnconfiguredHandlers()).isTrue();
		assertThat(properties.getDatabase()).isSameAs(database);
		assertThat(properties.getWorker()).isSameAs(worker);
		assertThat(properties.getCleanup()).isSameAs(cleanup);
		assertThat(properties.getExecutor()).isSameAs(executor);
		assertThat(properties.getRetry()).isSameAs(retry);
		assertThat(properties.getTaskTypes()).isSameAs(taskTypes);
	}

	@Test
	void nestedSettersRoundTripConfiguredValues() {
		MangoSwarmProperties properties = new MangoSwarmProperties();

		properties.getDatabase().setSchema("app_schema");
		properties.getDatabase().setApplySchemaToHibernateDefault(false);
		properties.getWorker().setHeartbeatInterval(Duration.ofSeconds(2));
		properties.getWorker().setStaleAfter(Duration.ofSeconds(9));
		properties.getCleanup().setEnabled(false);
		properties.getCleanup().setInterval(Duration.ofMinutes(2));
		properties.getCleanup().setCompletedRetention(Duration.ofDays(7));
		properties.getCleanup().setFailedRetention(Duration.ofDays(8));
		properties.getCleanup().setBatchSize(500);
		properties.getExecutor().setMaxThreads("32");
		properties.getExecutor().setPollInterval(Duration.ofMillis(25));
		properties.getExecutor().setQueueStrategy(MangoSwarmProperties.QueueStrategy.ABORT);
		properties.getExecutor().setVirtualThreads(MangoSwarmProperties.VirtualThreads.DISABLED);
		properties.getRetry().setBaseDelay(Duration.ofSeconds(3));
		properties.getRetry().setMultiplier(3.5d);
		properties.getRetry().setMaxDelay(Duration.ofMinutes(4));

		assertThat(properties.getDatabase().getSchema()).isEqualTo("app_schema");
		assertThat(properties.getDatabase().isApplySchemaToHibernateDefault()).isFalse();
		assertThat(properties.getWorker().getHeartbeatInterval()).isEqualTo(Duration.ofSeconds(2));
		assertThat(properties.getWorker().getStaleAfter()).isEqualTo(Duration.ofSeconds(9));
		assertThat(properties.getCleanup().isEnabled()).isFalse();
		assertThat(properties.getCleanup().getInterval()).isEqualTo(Duration.ofMinutes(2));
		assertThat(properties.getCleanup().getCompletedRetention()).isEqualTo(Duration.ofDays(7));
		assertThat(properties.getCleanup().getFailedRetention()).isEqualTo(Duration.ofDays(8));
		assertThat(properties.getCleanup().getBatchSize()).isEqualTo(500);
		assertThat(properties.getExecutor().getMaxThreads()).isEqualTo("32");
		assertThat(properties.getExecutor().getPollInterval()).isEqualTo(Duration.ofMillis(25));
		assertThat(properties.getExecutor().getQueueStrategy()).isEqualTo(MangoSwarmProperties.QueueStrategy.ABORT);
		assertThat(properties.getExecutor().getVirtualThreads())
				.isEqualTo(MangoSwarmProperties.VirtualThreads.DISABLED);
		assertThat(properties.getRetry().getBaseDelay()).isEqualTo(Duration.ofSeconds(3));
		assertThat(properties.getRetry().getMultiplier()).isEqualTo(3.5d);
		assertThat(properties.getRetry().getMaxDelay()).isEqualTo(Duration.ofMinutes(4));
	}

	@Test
	void taskTypeSettersRoundTripConfiguredValues() {
		MangoSwarmProperties.TaskType taskType = new MangoSwarmProperties.TaskType();

		assertThat(taskType.getMode()).isEqualTo(MangoSwarmProperties.TaskMode.EXECUTE);
		taskType.setMode(MangoSwarmProperties.TaskMode.QUEUE);
		taskType.setRate(50);
		taskType.setPeriod(Duration.ofSeconds(5));
		taskType.setConcurrency(6);
		taskType.setTimeout(Duration.ofSeconds(7));
		taskType.setReclaimOnTimeout(true);
		taskType.setIdempotent(true);
		taskType.setBatchSize(8);
		taskType.setMaxAttempts(9);
		taskType.setRetryBaseDelay(Duration.ofSeconds(10));
		taskType.setRetryMultiplier(1.5d);
		taskType.setRetryMaxDelay(Duration.ofSeconds(11));
		taskType.setRetryDelay(Duration.ofSeconds(12));

		assertThat(taskType.getMode()).isEqualTo(MangoSwarmProperties.TaskMode.QUEUE);
		assertThat(taskType.getRate()).isEqualTo(50);
		assertThat(taskType.getPeriod()).isEqualTo(Duration.ofSeconds(5));
		assertThat(taskType.getConcurrency()).isEqualTo(6);
		assertThat(taskType.getTimeout()).isEqualTo(Duration.ofSeconds(7));
		assertThat(taskType.isReclaimOnTimeout()).isTrue();
		assertThat(taskType.isIdempotent()).isTrue();
		assertThat(taskType.getBatchSize()).isEqualTo(8);
		assertThat(taskType.getMaxAttempts()).isEqualTo(9);
		assertThat(taskType.getRetryBaseDelay()).isEqualTo(Duration.ofSeconds(10));
		assertThat(taskType.getRetryMultiplier()).isEqualTo(1.5d);
		assertThat(taskType.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(11));
		assertThat(taskType.getRetryDelay()).isEqualTo(Duration.ofSeconds(12));
	}
}
