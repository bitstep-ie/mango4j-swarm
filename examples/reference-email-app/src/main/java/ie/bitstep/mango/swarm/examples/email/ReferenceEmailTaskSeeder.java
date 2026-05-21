package ie.bitstep.mango.examples.email;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie.bitstep.mango.swarm.MangoTasks;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "reference.email", name = "seed-on-startup", havingValue = "true", matchIfMissing = true)
class ReferenceEmailTaskSeeder implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(ReferenceEmailTaskSeeder.class);

    private final MangoTasks mangoTasks;
    private final ObjectMapper objectMapper;
    private final int seedCount;

    ReferenceEmailTaskSeeder(
            MangoTasks mangoTasks,
            ObjectMapper objectMapper,
            @Value("${reference.email.seed-count:20}") int seedCount) {
        this.mangoTasks = mangoTasks;
        this.objectMapper = objectMapper;
        this.seedCount = seedCount;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (int i = 1; i <= seedCount; i++) {
            ObjectNode payload = objectMapper.createObjectNode()
                    .put("customerId", "customer-%03d".formatted(i))
                    .put("to", "customer%03d@example.com".formatted(i))
                    .put("subject", "Reference email task %02d".formatted(i))
                    .put("body", "The reference handler logs this request instead of sending email.");

            UUID taskId = mangoTasks.enqueue("send-email", payload);
            log.debug("queued send-email reference task: taskId={}, sequence={}/{}", taskId, i, seedCount);
        }
        ObjectNode futurePayload = objectMapper.createObjectNode()
                .put("customerId", "customer-scheduled")
                .put("to", "scheduled@example.com")
                .put("subject", "Scheduled reference email task")
                .put("body", "This task was queued on startup but should only become available later.");

        UUID futureTaskId = mangoTasks.scheduleAfter("send-email", futurePayload, Duration.ofSeconds(30));
        log.debug("scheduled future send-email reference task: taskId={}, delay={}", futureTaskId, Duration.ofSeconds(30));
    }
}
