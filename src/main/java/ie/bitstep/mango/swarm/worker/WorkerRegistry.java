package ie.bitstep.mango.swarm.worker;

import java.time.Instant;
import java.util.UUID;

/** Persistence contract for distributed worker heartbeats and active worker counting. */
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
