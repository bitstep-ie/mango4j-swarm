package ie.bitstep.mango.swarm.handler;

import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;

public interface TaskHandler<T> {
    String taskType();

    PayloadExtractor<T> payloadExtractor();

    TaskExecutionResult execute(T payload, TaskExecutionContext context) throws Exception;
}
