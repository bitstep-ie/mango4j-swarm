package ie.bitstep.mango.swarm;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Runtime context passed to {@code TaskHandler} execution.
 *
 * @param <T> extracted payload type for the handler
 */
public final class TaskExecutionContext<T> {
    private final UUID taskId;
    private final String taskType;
    private final UUID workerId;
    private final int attemptCount;
    private final Instant claimedAt;
    private final T payload;
    private final ProgressReporter progressReporter;

    public TaskExecutionContext(
            UUID taskId,
            String taskType,
            UUID workerId,
            int attemptCount,
            Instant claimedAt,
            T payload,
            ProgressReporter progressReporter) {
        this.taskId = Objects.requireNonNull(taskId, "taskId must not be null");
        this.taskType = Objects.requireNonNull(taskType, "taskType must not be null");
        this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
        this.attemptCount = attemptCount;
        this.claimedAt = claimedAt;
        this.payload = payload;
        this.progressReporter = Objects.requireNonNull(progressReporter, "progressReporter must not be null");
    }

    /** @return unique task id */
    public UUID taskId() {
        return taskId;
    }

    /** @return configured task type key */
    public String taskType() {
        return taskType;
    }

    /** @return worker id currently executing the task */
    public UUID workerId() {
        return workerId;
    }

    /** @return current attempt count (1-based when first claimed) */
    public int attemptCount() {
        return attemptCount;
    }

    /** @return claim timestamp for this attempt */
    public Instant claimedAt() {
        return claimedAt;
    }

    /** @return extracted handler payload */
    public T payload() {
        return payload;
    }

    /**
     * Records progress without a textual stage description.
     *
     * @param percent progress percentage in range {@code [0,100]}
     */
    public void progress(int percent) {
        progress(percent, null);
    }

    /**
     * Records progress and an optional textual stage description.
     * <p>
     * Each progress update is treated as task liveness and extends timeout-reclaim detection.
     *
     * @param percent progress percentage in range {@code [0,100]}
     * @param description optional stage description, e.g. {@code "connecting"} or {@code "sending"}
     */
    public void progress(int percent, String description) {
        if (percent < 0 || percent > 100) {
            throw new IllegalArgumentException("progress percent must be between 0 and 100");
        }
        progressReporter.record(percent, description);
    }

    /** Internal callback used by the executor to persist progress updates. */
    @FunctionalInterface
    public interface ProgressReporter {
        void record(int percent, String description);
    }
}
