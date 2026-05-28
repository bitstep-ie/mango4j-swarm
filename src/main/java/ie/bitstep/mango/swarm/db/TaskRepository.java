package ie.bitstep.mango.swarm.db;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Internal persistence SPI for task lifecycle operations.
 *
 * <p>The default implementation is PostgreSQL-backed. Custom implementations may replace the auto-configured bean, but
 * they must preserve the same concurrent claiming and lifecycle semantics. Implementations must be safe for concurrent
 * use by multiple workers.
 */
public interface TaskRepository {
	/** Persists a task at a concrete availability time. */
	UUID queue(String taskType, JsonNode payload, Instant availableAt);

	/** Persists a task in the next available slot at or after a requested time. */
	UUID queueInNextSlot(String taskType, JsonNode payload, Instant requestedAt, Duration slotSpacing);

	/** Claims up to {@code limit} queued tasks for a worker. */
	List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit);

	/** Marks a claimed task as in progress. */
	void markInProgress(UUID taskId, UUID workerId, Instant now);

	/** Records task progress and textual stage while in progress. */
	void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description);

	/** Marks a task as completed. */
	void markCompleted(UUID taskId, UUID workerId, Instant now);

	/** Marks a task as failed. */
	void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage);

	/** Reschedules a failed attempt for retry using the same task row. */
	void rescheduleAfterFailure(UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage);

	/** Returns a claimed task back to queued when the local worker cannot dispatch it. */
	void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason);

	/** Requeues up to {@code limit} timed-out tasks for retry. */
	int reclaimTimedOut(String taskType, Duration timeout, Instant now, int limit);

	/** Marks up to {@code limit} timed-out tasks as failed when reclaim is disabled. */
	int markTimedOutFailed(String taskType, Duration timeout, Instant now, int limit);

	/** Deletes up to {@code limit} completed tasks older than the retention window. */
	int deleteCompletedOlderThan(Duration retention, Instant now, int limit);

	/** Deletes up to {@code limit} failed tasks older than the retention window. */
	int deleteFailedOlderThan(Duration retention, Instant now, int limit);

	/** Deletes up to {@code limit} task pacing slots older than the retention window. */
	int deleteTaskPacersOlderThan(Duration retention, Instant now, int limit);
}
