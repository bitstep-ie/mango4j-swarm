package ie.bitstep.mango.swarm;

/**
 * Outcome returned by a {@code TaskHandler}.
 *
 * <p>Returning {@link #completed()} marks the task successful. Returning {@link #failed(String)} triggers retry/failure
 * flow based on task-type configuration.
 */
public final class TaskExecutionResult {
	private static final TaskExecutionResult COMPLETED_RESULT = new TaskExecutionResult(true, null);

	private final boolean completed;
	private final String message;

	private TaskExecutionResult(boolean completed, String message) {
		this.completed = completed;
		this.message = message;
	}

	/** @return successful execution result */
	public static TaskExecutionResult completed() {
		return COMPLETED_RESULT;
	}

	/**
	 * Creates an explicit failure result.
	 *
	 * @param message error detail persisted for diagnostics
	 * @return failed result
	 */
	public static TaskExecutionResult failed(String message) {
		return new TaskExecutionResult(false, message);
	}

	/** @return {@code true} when execution is successful */
	public boolean isCompleted() {
		return completed;
	}

	/** @return optional failure message */
	public String message() {
		return message;
	}
}
