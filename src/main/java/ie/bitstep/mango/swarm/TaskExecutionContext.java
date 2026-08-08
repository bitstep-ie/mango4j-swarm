package ie.bitstep.mango.swarm;

import java.time.Duration;
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
	private final UUID seriesId;
	private final ProgressReporter progressReporter;
	private final AgainRequester againRequester;

	public TaskExecutionContext(
			UUID taskId,
			String taskType,
			UUID workerId,
			int attemptCount,
			Instant claimedAt,
			T payload,
			UUID seriesId,
			ProgressReporter progressReporter,
			AgainRequester againRequester) {
		this.taskId = Objects.requireNonNull(taskId, "taskId must not be null");
		this.taskType = Objects.requireNonNull(taskType, "taskType must not be null");
		this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
		this.attemptCount = attemptCount;
		this.claimedAt = claimedAt;
		this.payload = payload;
		this.seriesId = seriesId;
		this.progressReporter = Objects.requireNonNull(progressReporter, "progressReporter must not be null");
		this.againRequester = Objects.requireNonNull(againRequester, "againRequester must not be null");
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
	 * @return the id shared by every occurrence of this task's recurring series, or {@code null} if this task is
	 *     standalone (it did not originate from a prior {@link #again(Duration)} call)
	 */
	public UUID seriesId() {
		return seriesId;
	}

	/**
	 * Records progress without a textual stage description.
	 *
	 * @param percent progress percentage in range {@code [0,100]}
	 */
	public void progress(int percent) {
		updateProgress(percent, null);
	}

	/**
	 * Records progress and an optional textual stage description.
	 *
	 * <p>Each progress update is treated as task liveness and extends timeout-reclaim detection.
	 *
	 * @param percent progress percentage in range {@code [0,100]}
	 * @param description optional stage description, e.g. {@code "connecting"} or {@code "sending"}
	 */
	public void progress(int percent, String description) {
		updateProgress(percent, description);
	}

	/** Records the current progress percentage and message. */
	private void updateProgress(int percent, String message) {
		if (percent < 0 || percent > 100) {
			throw new IllegalArgumentException("progress percent must be between 0 and 100");
		}
		progressReporter.report("running", percent, message);
	}

	/**
	 * Requests a follow-up occurrence of this recurring task, reusing the current payload.
	 *
	 * <p>Safe to call from {@link ie.bitstep.mango.swarm.handler.TaskHandler#execute(TaskExecutionContext)} regardless
	 * of what the handler ultimately returns or throws — on success, on failure, or both, at the handler's discretion.
	 * The follow-up is inserted immediately as a new task row linked to this one via {@link #seriesId()}; it does not
	 * affect the outcome recorded for the current attempt.
	 *
	 * @param delay delay from now before the new occurrence becomes eligible
	 * @return the new occurrence's persisted task id
	 */
	public UUID again(Duration delay) {
		return again(delay, payload);
	}

	/**
	 * Requests a follow-up occurrence of this recurring task with a new payload, e.g. an advanced cursor.
	 *
	 * @param delay delay from now before the new occurrence becomes eligible
	 * @param newPayload payload for the new occurrence
	 * @return the new occurrence's persisted task id
	 * @see #again(Duration)
	 */
	public UUID again(Duration delay, T newPayload) {
		Objects.requireNonNull(delay, "delay must not be null");
		Objects.requireNonNull(newPayload, "newPayload must not be null");
		return againRequester.requestAgain(delay, newPayload);
	}

	/** Internal callback used by the executor to persist progress updates. */
	@FunctionalInterface
	public interface ProgressReporter {
		void report(String state, Integer percent, String message);
	}

	/** Internal callback used by the executor to insert a linked follow-up occurrence for {@link #again}. */
	@FunctionalInterface
	public interface AgainRequester {
		UUID requestAgain(Duration delay, Object payload);
	}
}
