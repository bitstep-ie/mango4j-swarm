package ie.bitstep.mango.swarm.handler;

/** Checked exception for task handler execution failures. */
public class TaskHandlerException extends Exception {
	public TaskHandlerException(String message, Throwable cause) {
		super(message, cause);
	}
}
