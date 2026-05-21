package ie.bitstep.mango.swarm.db;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface TaskRepository {
    UUID enqueue(String taskType, JsonNode payload, Instant availableAt);

    UUID enqueueInNextSlot(String taskType, JsonNode payload, Instant requestedAt, Duration slotSpacing);

    List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit);

    void markInProgress(UUID taskId, UUID workerId, Instant now);

    void markCompleted(UUID taskId, UUID workerId, Instant now);

    void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage);

    void rescheduleAfterFailure(UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage);

    int reclaimTimedOut(String taskType, Duration timeout, Instant now);

    int markTimedOutFailed(String taskType, Duration timeout, Instant now);
}
