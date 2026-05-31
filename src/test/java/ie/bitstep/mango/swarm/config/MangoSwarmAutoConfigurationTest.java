package ie.bitstep.mango.swarm.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.jdbc.core.JdbcTemplate;
import ie.bitstep.mango.swarm.MangoTasks;
import ie.bitstep.mango.swarm.db.SchemaQualifiedTables;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.executor.MangoSwarmDaemon;
import ie.bitstep.mango.swarm.handler.TaskHandlerRegistry;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MangoSwarmAutoConfigurationTest {

	private final MangoSwarmAutoConfiguration configuration = new MangoSwarmAutoConfiguration();

	@Test
	void createsCoreBeansFromCollaborators() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getExecutor().setMaxThreads("1");
		TaskRepository taskRepository = mock(TaskRepository.class);
		WorkerRegistry workerRegistry = mock(WorkerRegistry.class);
		ObjectMapper objectMapper = configuration.mangoObjectMapper();
		TaskHandlerRegistry registry = configuration.mangoTaskHandlerRegistry(List.of(), properties);

		MangoTasks tasks = configuration.mangoTasks(taskRepository, objectMapper, properties);
		MangoSwarmDaemon daemon = configuration.mangoSwarmDaemon(workerRegistry, taskRepository, registry, properties);

		assertThat(objectMapper).isNotNull();
		assertThat(registry.taskTypes()).isEmpty();
		assertThat(tasks).isNotNull();
		assertThat(daemon).isNotNull();
		daemon.stop();
	}

	@Test
	void schemaQualifiedTablesUseConfiguredSchema() {
		MangoSwarmProperties properties = properties();
		properties.getDatabase().setSchema(" app_schema ");

		SchemaQualifiedTables tables = configuration.mangoSchemaQualifiedTables(properties);

		assertThat(tables.schema()).isEqualTo("app_schema");
	}

	@Test
	void createsJdbcRepositoryAndWorkerRegistryBeans() {
		MangoSwarmProperties properties = properties();
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		ObjectMapper objectMapper = new ObjectMapper();
		SchemaQualifiedTables tables = new SchemaQualifiedTables(null);

		WorkerRegistry workerRegistry = configuration.mangoWorkerRegistry(jdbcTemplate, properties, tables);
		TaskRepository taskRepository = configuration.mangoTaskRepository(jdbcTemplate, objectMapper, tables);

		assertThat(workerRegistry).isNotNull();
		assertThat(taskRepository).isNotNull();
	}

	@Test
	void hibernateCustomizerAppliesTrimmedSchemaWhenMissing() {
		MangoSwarmProperties properties = properties();
		properties.getDatabase().setSchema(" app_schema ");
		HibernatePropertiesCustomizer customizer = configuration.mangoHibernateDefaultSchemaCustomizer(properties);
		HashMap<String, Object> hibernate = new HashMap<>();

		customizer.customize(hibernate);

		assertThat(hibernate).containsEntry("hibernate.default_schema", "app_schema");
	}

	@Test
	void hibernateCustomizerLeavesExistingOrDisabledSchemaAlone() {
		MangoSwarmProperties properties = properties();
		properties.getDatabase().setSchema("app_schema");
		HashMap<String, Object> existing = new HashMap<>();
		existing.put("hibernate.default_schema", "existing");

		configuration.mangoHibernateDefaultSchemaCustomizer(properties).customize(existing);

		properties.getDatabase().setApplySchemaToHibernateDefault(false);
		HashMap<String, Object> disabled = new HashMap<>();
		configuration.mangoHibernateDefaultSchemaCustomizer(properties).customize(disabled);

		properties.getDatabase().setSchema(" ");
		properties.getDatabase().setApplySchemaToHibernateDefault(true);
		HashMap<String, Object> blank = new HashMap<>();
		configuration.mangoHibernateDefaultSchemaCustomizer(properties).customize(blank);

		assertThat(existing).containsEntry("hibernate.default_schema", "existing");
		assertThat(disabled).doesNotContainKey("hibernate.default_schema");
		assertThat(blank).doesNotContainKey("hibernate.default_schema");
	}

	private static MangoSwarmProperties properties() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		properties.getExecutor().setMaxThreads("1");
		properties.getTaskTypes().put("email", taskType());
		return properties;
	}

	private static MangoSwarmProperties.TaskType taskType() {
		MangoSwarmProperties.TaskType taskType = new MangoSwarmProperties.TaskType();
		taskType.setRate(1);
		taskType.setPeriod(Duration.ofSeconds(1));
		return taskType;
	}
}
