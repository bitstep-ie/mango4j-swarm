package ie.bitstep.mango.swarm.worker;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import ie.bitstep.mango.swarm.PostgresTestSupport;
import ie.bitstep.mango.swarm.db.SchemaQualifiedTables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerRegistryTest extends PostgresTestSupport {

	@Test
	void registersWorkerAndUpdatesHeartbeat() {
		UUID workerId = UUID.randomUUID();
		Instant started = Instant.parse("2026-05-20T10:00:00Z");

		int count = workerRegistry.heartbeat(workerId, "node-a", started, started);
		workerRegistry.heartbeat(workerId, "node-a", started, started.plusSeconds(5));

		Integer rows = jdbcTemplate.queryForObject("select count(*) from mango_swarm_workers", Integer.class);
		Instant lastHeartbeat = jdbcTemplate
				.queryForObject(
						"select last_heartbeat_at from mango_swarm_workers where worker_id = ?",
						OffsetDateTime.class,
						workerId)
				.toInstant();

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

	@Test
	void heartbeatInsertsNewWorkerWhenUpdateFindsNoExistingRow() throws Exception {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		java.sql.Connection connection = mock(java.sql.Connection.class);
		java.sql.PreparedStatement updateStatement = mock(java.sql.PreparedStatement.class);
		java.sql.PreparedStatement insertStatement = mock(java.sql.PreparedStatement.class);
		java.sql.PreparedStatement deleteStatement = mock(java.sql.PreparedStatement.class);
		java.sql.PreparedStatement countStatement = mock(java.sql.PreparedStatement.class);
		java.sql.ResultSet resultSet = mock(java.sql.ResultSet.class);
		when(connection.prepareStatement(contains("UPDATE mango_swarm_workers SET")))
				.thenReturn(updateStatement);
		when(connection.prepareStatement(contains("INSERT INTO mango_swarm_workers")))
				.thenReturn(insertStatement);
		when(connection.prepareStatement(contains("DELETE FROM mango_swarm_workers")))
				.thenReturn(deleteStatement);
		when(connection.prepareStatement(contains("SELECT count(*)"))).thenReturn(countStatement);
		when(updateStatement.executeUpdate()).thenReturn(0);
		when(insertStatement.executeUpdate()).thenReturn(1);
		when(deleteStatement.executeUpdate()).thenReturn(0);
		when(countStatement.executeQuery()).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getInt(1)).thenReturn(1);
		when(jdbcTemplate.execute(org.mockito.ArgumentMatchers.<ConnectionCallback<Integer>>any()))
				.thenAnswer(invocation ->
						invocation.<ConnectionCallback<Integer>>getArgument(0).doInConnection(connection));
		JdbcWorkerRegistry registry =
				new JdbcWorkerRegistry(jdbcTemplate, java.time.Duration.ofSeconds(30), new SchemaQualifiedTables(null));
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		assertThatCode(() -> registry.heartbeat(UUID.randomUUID(), "node-a", now, now))
				.doesNotThrowAnyException();

		verify(updateStatement).executeUpdate();
		verify(insertStatement).executeUpdate();
	}

	@Test
	void heartbeatCallbacksReturnAffectedRowCounts() throws Exception {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		java.sql.Connection connection = mock(java.sql.Connection.class);
		java.sql.PreparedStatement statement = mock(java.sql.PreparedStatement.class);
		java.sql.ResultSet resultSet = mock(java.sql.ResultSet.class);
		when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
				.thenReturn(statement);
		when(statement.executeUpdate()).thenReturn(1, 1);
		when(statement.executeQuery()).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getInt(1)).thenReturn(2);
		java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
		when(jdbcTemplate.execute(org.mockito.ArgumentMatchers.<ConnectionCallback<Integer>>any()))
				.thenAnswer(invocation -> {
					Integer result = invocation
							.<ConnectionCallback<Integer>>getArgument(0)
							.doInConnection(connection);
					int call = calls.incrementAndGet();
					if (call == 1 || call == 2) {
						assertThat(result).isEqualTo(1);
					}
					if (call == 3) {
						assertThat(result).isEqualTo(2);
					}
					return result;
				});
		JdbcWorkerRegistry registry =
				new JdbcWorkerRegistry(jdbcTemplate, java.time.Duration.ofSeconds(30), new SchemaQualifiedTables(null));
		Instant now = Instant.parse("2026-05-20T10:00:00Z");

		int activeWorkers = registry.heartbeat(UUID.randomUUID(), "node-a", now, now);

		assertThat(activeWorkers).isEqualTo(2);
		assertThat(calls.get()).isEqualTo(3);
	}
}
