package ie.bitstep.mango.swarm.db;

import com.fasterxml.jackson.databind.JsonNode;
import ie.bitstep.mango.swarm.TaskStatus;
import java.time.Instant;
import java.util.UUID;

public record TaskRecord(
        UUID id,
        String taskType,
        JsonNode payload,
        TaskStatus status,
        Instant availableAt,
        UUID claimedBy,
        Instant claimedAt,
        int attemptCount,
        Instant createdAt,
        Instant updatedAt
) {
}
