package ie.bitstep.mango.swarm.worker;

import java.time.Instant;
import java.util.UUID;

public interface WorkerRegistry {
    int heartbeat(UUID workerId, String hostname, Instant startedAt, Instant now);

    int countActiveWorkers(Instant now);
}
