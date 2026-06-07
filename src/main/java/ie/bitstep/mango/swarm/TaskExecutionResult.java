package ie.bitstep.mango.swarm;

/**
 * Outcome returned by a {@code TaskHandler}.
 *
 * <p>Return {@link #completed()} on success or {@link #failed(String)} to trigger the retry/failure flow based on
 * task-type configuration.
 */
public sealed interface TaskExecutionResult permits TaskExecutionResult.Completed, TaskExecutionResult.Failed {

    /** Successful execution marker. */
    record Completed() implements TaskExecutionResult {}

    /** Explicit failure with a diagnostic message persisted for diagnostics. */
    record Failed(String message) implements TaskExecutionResult {}

    /** @return successful execution result */
    static Completed completed() {
        return new Completed();
    }

    /**
     * Creates an explicit failure result.
     *
     * @param message error detail persisted for diagnostics
     * @return failed result
     */
    static Failed failed(String message) {
        return new Failed(message);
    }
}
