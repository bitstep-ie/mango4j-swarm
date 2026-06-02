package ie.bitstep.mango.swarm;

import java.time.Duration;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ie.bitstep.mango.swarm.db.JdbcTaskRepository;
import ie.bitstep.mango.swarm.db.TaskRepository;
import ie.bitstep.mango.swarm.worker.JdbcWorkerRegistry;
import ie.bitstep.mango.swarm.worker.WorkerRegistry;

public abstract class H2TestSupport {
	protected JdbcTemplate jdbcTemplate;
	protected WorkerRegistry workerRegistry;
	protected TaskRepository taskRepository;
	protected ObjectMapper objectMapper;

	@BeforeEach
	void setUpDatabase() {
		objectMapper = new ObjectMapper();
		DataSource dataSource = dataSource();
		jdbcTemplate = new JdbcTemplate(dataSource);
		resetSchema();
		workerRegistry = new JdbcWorkerRegistry(jdbcTemplate, Duration.ofSeconds(30));
		taskRepository = new JdbcTaskRepository(jdbcTemplate, objectMapper);
	}

	private DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setUrl(
				"jdbc:h2:mem:mango_swarm;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1");
		dataSource.setUsername("sa");
		dataSource.setPassword("");
		return dataSource;
	}

	private void resetSchema() {
		jdbcTemplate.execute("DROP ALL OBJECTS");
		jdbcTemplate.execute(
				"""
				CREATE TABLE mango_swarm_workers (
					worker_id uuid PRIMARY KEY,
					hostname text,
					started_at timestamp NOT NULL,
					last_heartbeat_at timestamp NOT NULL
				)
				""");
		jdbcTemplate.execute(
				"""
				CREATE TABLE mango_swarm_task_pacers (
					task_type text NOT NULL,
					slot_at timestamp NOT NULL,
					task_id uuid NOT NULL,
					created_at timestamp NOT NULL DEFAULT now(),
					PRIMARY KEY (task_type, slot_at)
				)
				""");
		jdbcTemplate.execute(
				"""
				CREATE TABLE mango_swarm_tasks (
					id uuid PRIMARY KEY,
					task_type text NOT NULL,
					payload text NOT NULL,
					status text NOT NULL CHECK (status IN ('queued', 'claimed', 'in_progress', 'completed', 'failed')),
					available_at timestamp NOT NULL DEFAULT now(),
					claimed_by uuid NULL,
					claimed_at timestamp NULL,
					attempt_count integer NOT NULL DEFAULT 0,
					created_at timestamp NOT NULL DEFAULT now(),
					updated_at timestamp NOT NULL DEFAULT now(),
					completed_at timestamp NULL,
					failed_at timestamp NULL,
					execution_time_ms bigint NULL CHECK (execution_time_ms >= 0),
					last_error_message text NULL
				)
				""");
		jdbcTemplate.execute(
				"""
				CREATE TABLE mango_swarm_task_runtime (
					task_id uuid PRIMARY KEY,
					worker_id uuid NOT NULL,
					execution_state text NOT NULL,
					progress_percent integer NULL CHECK (progress_percent BETWEEN 0 AND 100),
					progress_message text NULL,
					started_at timestamp NOT NULL,
					updated_at timestamp NOT NULL,
					execution_time_ms bigint NOT NULL DEFAULT 0 CHECK (execution_time_ms >= 0),
					FOREIGN KEY (task_id) REFERENCES mango_swarm_tasks(id) ON DELETE CASCADE
				)
				""");
		jdbcTemplate.execute(
				"""
				CREATE INDEX idx_mango_tasks_queue_claim
					ON mango_swarm_tasks (task_type, status, available_at, id)
				""");
		jdbcTemplate.execute(
				"""
				CREATE INDEX idx_mango_task_runtime_worker
					ON mango_swarm_task_runtime (worker_id, task_id)
				""");
		jdbcTemplate.execute(
				"""
				CREATE INDEX idx_mango_tasks_timeout_due
					ON mango_swarm_tasks (task_type, status, claimed_at, id)
				""");
		jdbcTemplate.execute(
				"""
				CREATE INDEX idx_mango_tasks_completed_cleanup
					ON mango_swarm_tasks (status, completed_at, id)
				""");
		jdbcTemplate.execute(
				"""
				CREATE INDEX idx_mango_tasks_failed_cleanup
					ON mango_swarm_tasks (status, failed_at, id)
				""");
	}
}
