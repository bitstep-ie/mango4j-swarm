package ie.bitstep.mango.swarm.worker;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import ie.bitstep.mango.swarm.db.SchemaQualifiedTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcWorkerRegistry implements WorkerRegistry {
    private static final Logger log = LoggerFactory.getLogger(JdbcWorkerRegistry.class);

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
        jdbcTemplate.update("""
                INSERT INTO %s(worker_id, hostname, started_at, last_heartbeat_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (worker_id) DO UPDATE
                SET hostname = EXCLUDED.hostname,
                    last_heartbeat_at = EXCLUDED.last_heartbeat_at
                """.formatted(tables.workers()), workerId, hostname, Timestamp.from(startedAt), Timestamp.from(now));
        Instant staleBefore = now.minus(staleAfter);
        int prunedWorkers = jdbcTemplate.update("DELETE FROM " + tables.workers() + " WHERE last_heartbeat_at < ?",
                Timestamp.from(staleBefore));
        if (prunedWorkers > 0) {
            log.debug(
                    "swarm pruned stale workers: table={}, staleBefore={}, staleAfter={}, pruned={}",
                    tables.workers(),
                    staleBefore,
                    staleAfter,
                    prunedWorkers);
        } else {
            log.debug(
                    "swarm worker cleanup complete: table={}, staleBefore={}, staleAfter={}, pruned=0",
                    tables.workers(),
                    staleBefore,
                    staleAfter);
        }
        return countActiveWorkers(now);
    }

    @Override
    public int countActiveWorkers(Instant now) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM " + tables.workers() + " WHERE last_heartbeat_at >= ?",
                Integer.class,
                Timestamp.from(now.minus(staleAfter)));
        return Math.max(count == null ? 0 : count, 1);
    }
}
