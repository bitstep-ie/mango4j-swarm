package ie.bitstep.mango.swarm;

public final class TaskExecutionResult {
    private static final TaskExecutionResult COMPLETED = new TaskExecutionResult(true, null);

    private final boolean completed;
    private final String message;

    private TaskExecutionResult(boolean completed, String message) {
        this.completed = completed;
        this.message = message;
    }

    public static TaskExecutionResult completed() {
        return COMPLETED;
    }

    public static TaskExecutionResult failed(String message) {
        return new TaskExecutionResult(false, message);
    }

    public boolean isCompleted() {
        return completed;
    }

    public String message() {
        return message;
    }
}
