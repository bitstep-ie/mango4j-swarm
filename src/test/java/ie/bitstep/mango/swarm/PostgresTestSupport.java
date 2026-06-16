package ie.bitstep.mango.swarm;

import java.time.Duration;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ie.bitstep.mango.swarm.db.JdbcTaskRepository;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.worker.JdbcWorkerRegistry;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;

@Testcontainers(disabledWithoutDocker = true)
public abstract class PostgresTestSupport {
	@Container
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

	protected JdbcTemplate jdbcTemplate;
	protected WorkerRegistry workerRegistry;
	protected TaskRepository taskRepository;
	protected ObjectMapper objectMapper;

	@BeforeEach
	void setUpDatabase() {
		objectMapper = new ObjectMapper();
		DataSource dataSource = dataSource();
		Flyway.configure()
				.dataSource(dataSource)
				.locations("classpath:db/migration")
				.cleanDisabled(false)
				.load()
				.clean();
		Flyway.configure()
				.dataSource(dataSource)
				.locations("classpath:db/migration")
				.load()
				.migrate();
		jdbcTemplate = new JdbcTemplate(dataSource);
		workerRegistry = new JdbcWorkerRegistry(jdbcTemplate, Duration.ofSeconds(30));
		taskRepository = new JdbcTaskRepository(jdbcTemplate, objectMapper);
	}

	private DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setUrl(POSTGRES.getJdbcUrl());
		dataSource.setUsername(POSTGRES.getUsername());
		dataSource.setPassword(POSTGRES.getPassword());
		return dataSource;
	}
}
