package ie.bitstep.mango.swarm;

import java.time.Instant;
import java.util.UUID;

public record TaskExecutionContext(
        UUID taskId,
        String taskType,
        UUID workerId,
        int attemptCount,
        Instant claimedAt
) {
}
