package ie.bitstep.mango.swarm.worker;

import java.time.Instant;
import java.util.UUID;

/**
 * Internal worker coordination SPI for distributed heartbeats and active worker counting.
 *
 * <p>The default implementation is PostgreSQL-backed. Custom implementations may replace the auto-configured bean, but
 * they must preserve stale-worker pruning and active-worker counting semantics.
 */
public interface WorkerRegistry {
	/**
	 * Upserts a worker heartbeat and prunes stale workers.
	 *
	 * @return current number of active workers after pruning
	 */
	int heartbeat(UUID workerId, String hostname, Instant startedAt, Instant now);

	/** @return number of active workers at the provided instant */
	int countActiveWorkers(Instant now);
}
