package ie.bitstep.mango.swarm.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import ie.bitstep.mango.swarm.MangoTasks;
import ie.bitstep.mango.swarm.db.JdbcTaskRepository;
import ie.bitstep.mango.swarm.db.SchemaQualifiedTables;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.executor.MangoSwarmDaemon;
import ie.bitstep.mango.swarm.handler.TaskHandler;
import ie.bitstep.mango.swarm.handler.TaskHandlerRegistry;
import ie.bitstep.mango.swarm.worker.JdbcWorkerRegistry;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;
import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

@AutoConfiguration
@EnableConfigurationProperties(MangoSwarmProperties.class)
@ConditionalOnProperty(prefix = "mango.swarm", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MangoSwarmAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    ObjectMapper mangoObjectMapper() {
        return new ObjectMapper();
    }

    @Bean
    @ConditionalOnMissingBean
    SchemaQualifiedTables mangoSchemaQualifiedTables(MangoSwarmProperties properties) {
        return new SchemaQualifiedTables(properties.getDatabase().getSchema());
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerRegistry mangoWorkerRegistry(
            JdbcTemplate jdbcTemplate,
            MangoSwarmProperties properties,
            SchemaQualifiedTables tables) {
        return new JdbcWorkerRegistry(jdbcTemplate, properties.getWorker().getStaleAfter(), tables);
    }

    @Bean
    @ConditionalOnMissingBean
    TaskRepository mangoTaskRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, SchemaQualifiedTables tables) {
        return new JdbcTaskRepository(jdbcTemplate, objectMapper, tables);
    }

    @Bean
    @ConditionalOnMissingBean
    MangoTasks mangoTasks(
            TaskRepository taskRepository,
            ObjectMapper objectMapper,
            MangoSwarmProperties properties) {
        return new MangoTasks(taskRepository, objectMapper, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    TaskHandlerRegistry mangoTaskHandlerRegistry(List<TaskHandler<?>> handlers, MangoSwarmProperties properties) {
        return new TaskHandlerRegistry(handlers, properties.getTaskTypes().keySet(), properties.isAllowUnconfiguredHandlers());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnBean(TaskHandlerRegistry.class)
    @ConditionalOnMissingBean
    MangoSwarmDaemon mangoSwarmDaemon(
            WorkerRegistry workerRegistry,
            TaskRepository taskRepository,
            TaskHandlerRegistry handlerRegistry,
            MangoSwarmProperties properties) {
        return new MangoSwarmDaemon(workerRegistry, taskRepository, handlerRegistry, properties);
    }

    @Bean
    @ConditionalOnClass(name = "org.hibernate.cfg.AvailableSettings")
    HibernatePropertiesCustomizer mangoHibernateDefaultSchemaCustomizer(MangoSwarmProperties properties) {
        return hibernateProperties -> {
            String schema = normalizedSchema(properties);
            if (schema != null
                    && properties.getDatabase().isApplySchemaToHibernateDefault()
                    && !hibernateProperties.containsKey("hibernate.default_schema")) {
                hibernateProperties.put("hibernate.default_schema", schema);
            }
        };
    }

    private static String normalizedSchema(MangoSwarmProperties properties) {
        String schema = properties.getDatabase().getSchema();
        if (schema == null || schema.isBlank()) {
            return null;
        }
        return schema.trim();
    }
}
