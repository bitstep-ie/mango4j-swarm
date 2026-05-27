package ie.bitstep.mango.examples.email;

import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.handler.TaskHandler;
import ie.bitstep.mango.swarm.handler.SwarmHandler;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SwarmHandler("send-email")
class SendEmailTaskHandler implements TaskHandler<EmailPayload> {
    private static final Logger log = LoggerFactory.getLogger(SendEmailTaskHandler.class);

    @Override
    public PayloadExtractor<EmailPayload> payloadExtractor() {
        return reader -> new EmailPayload(
                reader.required(String.class, "customerId", "userId", "customer.id"),
                reader.required(String.class, "to", "email", "recipientEmail", "to.address"),
                reader.optional(String.class, "subject").orDefault("Hello from mango4j"),
                reader.optional(String.class, "body").orDefault("This is a reference email task."));
    }

    @Override
    public TaskExecutionResult execute(TaskExecutionContext<EmailPayload> context) {
        EmailPayload payload = context.payload();
        context.progress(10, "preparing");
        log.info("send-email task fired: taskId={}, attempt={}, customerId={}, to={}, subject={}, body={}",
                context.taskId(),
                context.attemptCount(),
                payload.customerId(),
                payload.to(),
                payload.subject(),
                payload.body());
        return TaskExecutionResult.completed();
    }
}
