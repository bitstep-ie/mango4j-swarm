package ie.bitstep.mango.swarm.worker;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import ie.bitstep.mango.swarm.H2TestSupport;
import ie.bitstep.mango.swarm.db.SchemaQualifiedTables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkerRegistryTest extends H2TestSupport {

	@Test
	void registersWorkerAndUpdatesHeartbeat() {
		UUID workerId = UUID.randomUUID();
		Instant started = Instant.parse("2026-05-20T10:00:00Z");

		int count = workerRegistry.heartbeat(workerId, "node-a", started, started);
		workerRegistry.heartbeat(workerId, "node-a", started, started.plusSeconds(5));

		Integer rows = jdbcTemplate.queryForObject("select count(*) from mango_swarm_workers", Integer.class);
		Instant lastHeartbeat = jdbcTemplate.queryForObject(
				"select last_heartbeat_at from mango_swarm_workers where worker_id = ?", Instant.class, workerId);

		assertThat(count).isEqualTo(1);
		assertThat(rows).isEqualTo(1);
		assertThat(lastHeartbeat).isEqualTo(started.plusSeconds(5));
	}

	@Test
	void prunesStaleWorkersOnHeartbeat() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		UUID stale = UUID.randomUUID();
		UUID active = UUID.randomUUID();

		workerRegistry.heartbeat(stale, "stale", now, now.minusSeconds(60));
		int activeCount = workerRegistry.heartbeat(active, "active", now, now);

		Integer rows = jdbcTemplate.queryForObject("select count(*) from mango_swarm_workers", Integer.class);

		assertThat(activeCount).isEqualTo(1);
		assertThat(rows).isEqualTo(1);
	}

	@Test
	void countsMultipleActiveWorkersAndFallsBackToOneWhenNoRowsMatch() {
		Instant now = Instant.parse("2026-05-20T10:00:00Z");
		workerRegistry.heartbeat(UUID.randomUUID(), "node-a", now, now);
		workerRegistry.heartbeat(UUID.randomUUID(), "node-b", now, now);

		assertThat(workerRegistry.countActiveWorkers(now)).isEqualTo(2);
		assertThat(workerRegistry.countActiveWorkers(now.plusSeconds(60))).isEqualTo(1);
	}

	@Test
	void rejectsNullJdbcTemplateExecuteResult() {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		JdbcWorkerRegistry registry =
				new JdbcWorkerRegistry(jdbcTemplate, java.time.Duration.ofSeconds(30), new SchemaQualifiedTables(null));
		when(jdbcTemplate.execute(org.mockito.ArgumentMatchers.<ConnectionCallback<Integer>>any()))
				.thenReturn(null);
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		assertThatThrownBy(() -> registry.countActiveWorkers(now))
				.isInstanceOf(NullPointerException.class)
				.hasMessage("JdbcTemplate.execute returned null for count active workers");
	}
}
