package ie.bitstep.mango.swarm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRecord;
import ie.bitstep.mango.swarm.db.TaskRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MangoTasksTest {

    @Test
    void schedulesObjectPayloadAtRequestedTime() {
        RecordingRepository repository = new RecordingRepository();
        MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
        Instant availableAt = Instant.parse("2026-05-21T10:00:00Z");

        UUID taskId = tasks.schedule("email", new EmailRequest("customer-1", "x@example.com"), availableAt);

        assertThat(taskId).isEqualTo(RecordingRepository.TASK_ID);
        assertThat(repository.taskType).isEqualTo("email");
        assertThat(repository.availableAt).isEqualTo(availableAt);
        assertThat(repository.slotSpacing).isEqualTo(Duration.ofMillis(10));
        assertThat(repository.payload.get("customerId").asText()).isEqualTo("customer-1");
        assertThat(repository.payload.get("email").asText()).isEqualTo("x@example.com");
    }

    @Test
    void scheduleAfterUsesFutureAvailableAt() {
        RecordingRepository repository = new RecordingRepository();
        MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
        Instant before = Instant.now().plusSeconds(29);

        tasks.scheduleAfter("email", new EmailRequest("customer-1", "x@example.com"), Duration.ofSeconds(30));

        assertThat(repository.availableAt).isAfterOrEqualTo(before);
    }

    private static MangoSwarmProperties properties() {
        MangoSwarmProperties properties = new MangoSwarmProperties();
        MangoSwarmProperties.TaskType email = new MangoSwarmProperties.TaskType();
        email.setRate(100);
        email.setPeriod(Duration.ofSeconds(1));
        properties.getTaskTypes().put("email", email);
        return properties;
    }

    private record EmailRequest(String customerId, String email) {
    }

    private static final class RecordingRepository implements TaskRepository {
        private String taskType;
        private JsonNode payload;
        private Instant availableAt;
        private Duration slotSpacing;

        private static final UUID TASK_ID = UUID.fromString("018f0000-0000-7000-8000-000000000001");

        @Override
        public UUID enqueue(String taskType, JsonNode payload, Instant availableAt) {
            return enqueueInNextSlot(taskType, payload, availableAt, Duration.ZERO);
        }

        @Override
        public UUID enqueueInNextSlot(String taskType, JsonNode payload, Instant availableAt, Duration slotSpacing) {
            this.taskType = taskType;
            this.payload = payload;
            this.availableAt = availableAt;
            this.slotSpacing = slotSpacing;
            return TASK_ID;
        }

        @Override
        public List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit) {
            return List.of();
        }

        @Override
        public void markInProgress(UUID taskId, UUID workerId, Instant now) {
        }

        @Override
        public void markCompleted(UUID taskId, UUID workerId, Instant now) {
        }

        @Override
        public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {
        }

        @Override
        public void rescheduleAfterFailure(
                UUID taskId,
                UUID workerId,
                Instant now,
                Instant availableAt,
                String errorMessage) {
        }

        @Override
        public int reclaimTimedOut(String taskType, Duration timeout, Instant now) {
            return 0;
        }

        @Override
        public int markTimedOutFailed(String taskType, Duration timeout, Instant now) {
            return 0;
        }
    }
}
