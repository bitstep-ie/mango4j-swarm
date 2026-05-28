package ie.bitstep.mango.swarm.worker;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import ie.bitstep.mango.swarm.db.SchemaQualifiedTables;

/** JDBC/PostgreSQL implementation of {@link WorkerRegistry}. */
public class JdbcWorkerRegistry implements WorkerRegistry {
	private static final Logger log = LoggerFactory.getLogger(JdbcWorkerRegistry.class);
	private static final String UPDATE_WORKER_SQL =
			"""
			UPDATE mango_swarm_workers
			SET hostname = ?, last_heartbeat_at = ?
			WHERE worker_id = ?
			""";
	private static final String INSERT_WORKER_SQL =
			"""
			INSERT INTO mango_swarm_workers (worker_id, hostname, started_at, last_heartbeat_at)
			VALUES (?, ?, ?, ?)
			""";
	private static final String DELETE_STALE_WORKERS_SQL =
			"DELETE FROM mango_swarm_workers WHERE last_heartbeat_at < ?";
	private static final String COUNT_ACTIVE_WORKERS_SQL =
			"SELECT count(*) FROM mango_swarm_workers WHERE last_heartbeat_at >= ?";

	private final JdbcTemplate jdbcTemplate;
	private final Duration staleAfter;
	private final SchemaQualifiedTables tables;

	public JdbcWorkerRegistry(JdbcTemplate jdbcTemplate, Duration staleAfter) {
		this(jdbcTemplate, staleAfter, new SchemaQualifiedTables(null));
	}

	public JdbcWorkerRegistry(JdbcTemplate jdbcTemplate, Duration staleAfter, SchemaQualifiedTables tables) {
		this.jdbcTemplate = jdbcTemplate;
		this.staleAfter = staleAfter;
		this.tables = tables;
	}

	@Override
	public int heartbeat(UUID workerId, String hostname, Instant startedAt, Instant now) {
		int updated = executeRequired(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(UPDATE_WORKER_SQL)) {
						statement.setString(1, hostname);
						statement.setTimestamp(2, Timestamp.from(now));
						statement.setObject(3, workerId);
						return statement.executeUpdate();
					}
				}),
				"update worker heartbeat");
		if (updated == 0) {
			executeRequired(
					(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
						try (PreparedStatement statement = scoped.prepareStatement(INSERT_WORKER_SQL)) {
							statement.setObject(1, workerId);
							statement.setString(2, hostname);
							statement.setTimestamp(3, Timestamp.from(startedAt));
							statement.setTimestamp(4, Timestamp.from(now));
							return statement.executeUpdate();
						}
					}),
					"insert worker heartbeat");
		}
		Instant staleBefore = now.minus(staleAfter);
		int prunedWorkers = executeRequired(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(DELETE_STALE_WORKERS_SQL)) {
						statement.setTimestamp(1, Timestamp.from(staleBefore));
						return statement.executeUpdate();
					}
				}),
				"delete stale workers");
		log.debug(
				"swarm pruned stale workers: staleBefore={}, staleAfter={}, pruned={}",
				staleBefore,
				staleAfter,
				prunedWorkers);
		return countActiveWorkers(now);
	}

	@Override
	public int countActiveWorkers(Instant now) {
		int count = executeRequired(
				(ConnectionCallback<Integer>) connection -> tables.withSearchPath(connection, scoped -> {
					try (PreparedStatement statement = scoped.prepareStatement(COUNT_ACTIVE_WORKERS_SQL)) {
						statement.setTimestamp(1, Timestamp.from(now.minus(staleAfter)));
						try (ResultSet rs = statement.executeQuery()) {
							rs.next();
							return rs.getInt(1);
						}
					}
				}),
				"count active workers");
		return Math.max(count, 1);
	}

	private <T> T executeRequired(ConnectionCallback<T> callback, String operation) {
		return Objects.requireNonNull(
				jdbcTemplate.execute(callback), () -> "JdbcTemplate.execute returned null for " + operation);
	}
}
