package ie.bitstep.mango.swarm.handler;

import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;

/**
 * Application-defined task handler contract.
 *
 * @param <T> extracted payload type consumed by this handler
 */
public interface TaskHandler<T> {
	/** @return payload extractor used to evolve durable JSON payloads into the current Java model */
	PayloadExtractor<T> payloadExtractor();

	/**
	 * Executes a task attempt.
	 *
	 * @param context execution metadata, typed payload, and progress reporting API
	 * @return completed or failed result; returning {@code null} is treated as completed for backward compatibility,
	 *     but explicit {@link TaskExecutionResult#completed()} is preferred
	 * @throws TaskHandlerException unexpected checked execution failure
	 */
	TaskExecutionResult execute(TaskExecutionContext<T> context) throws TaskHandlerException;
}
