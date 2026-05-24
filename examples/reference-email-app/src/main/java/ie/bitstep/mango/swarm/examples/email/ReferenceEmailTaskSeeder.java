package ie.bitstep.mango.examples.email;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie.bitstep.mango.swarm.MangoTasks;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "reference.email", name = "seed-enabled", havingValue = "true", matchIfMissing = true)
class ReferenceEmailTaskSeeder implements SchedulingConfigurer {
    private static final Logger log = LoggerFactory.getLogger(ReferenceEmailTaskSeeder.class);

    private final MangoTasks mangoTasks;
    private final ObjectMapper objectMapper;
    private final ReferenceEmailSeedProperties seedProperties;
    private final AtomicLong sequence = new AtomicLong();

    ReferenceEmailTaskSeeder(
            MangoTasks mangoTasks,
            ObjectMapper objectMapper,
            ReferenceEmailSeedProperties seedProperties) {
        this.mangoTasks = mangoTasks;
        this.objectMapper = objectMapper;
        this.seedProperties = seedProperties;
    }

    void seedBatch() {
        int seedCount = seedProperties.seedCount();
        long firstSequence = sequence.incrementAndGet();
        for (int i = 0; i < seedCount; i++) {
            long current = i == 0 ? firstSequence : sequence.incrementAndGet();
            ObjectNode payload = objectMapper.createObjectNode()
                    .put("customerId", "customer-%06d".formatted(current))
                    .put("to", "customer%06d@example.com".formatted(current))
                    .put("subject", "Reference email task %06d".formatted(current))
                    .put("body", "The reference handler logs this request instead of sending email.");

            UUID taskId = mangoTasks.queue("send-email", payload);
            log.debug("queued send-email reference task: taskId={}, sequence={}", taskId, current);
        }
        if (seedProperties.scheduleFutureTask()) {
            ObjectNode futurePayload = objectMapper.createObjectNode()
                    .put("customerId", "customer-scheduled")
                    .put("to", "scheduled@example.com")
                    .put("subject", "Scheduled reference email task")
                    .put("body", "This task should only become available later.");
            UUID futureTaskId = mangoTasks.after(Duration.ofSeconds(30), "send-email", futurePayload);
            log.debug("scheduled future send-email reference task: taskId={}, delay={}", futureTaskId, Duration.ofSeconds(30));
        }
        log.info(
                "seeded send-email batch: count={}, interval={}, next-run-approx-in={}",
                seedCount,
                seedProperties.seedInterval(),
                seedProperties.seedInterval());
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addFixedDelayTask(new IntervalTask(
                this::seedBatch,
                seedProperties.seedInterval().toMillis(),
                seedProperties.seedInitialDelay().toMillis()));
    }
}
