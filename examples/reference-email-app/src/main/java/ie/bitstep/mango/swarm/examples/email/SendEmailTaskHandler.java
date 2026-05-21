package ie.bitstep.mango.examples.email;

import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.handler.TaskHandler;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
class SendEmailTaskHandler implements TaskHandler<EmailPayload> {
    private static final Logger log = LoggerFactory.getLogger(SendEmailTaskHandler.class);

    @Override
    public String taskType() {
        return "send-email";
    }

    @Override
    public PayloadExtractor<EmailPayload> payloadExtractor() {
        return reader -> new EmailPayload(
                reader.required(String.class, "customerId", "userId", "customer.id"),
                reader.required(String.class, "to", "email", "recipientEmail", "to.address"),
                reader.optional(String.class, "subject").orDefault("Hello from mango4j"),
                reader.optional(String.class, "body").orDefault("This is a reference email task."));
    }

    @Override
    public TaskExecutionResult execute(EmailPayload payload, TaskExecutionContext context) {
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
