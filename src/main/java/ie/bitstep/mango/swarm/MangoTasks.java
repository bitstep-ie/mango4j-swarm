package ie.bitstep.mango.swarm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class MangoTasks {
    private final TaskRepository taskRepository;
    private final ObjectMapper objectMapper;
    private final MangoSwarmProperties properties;

    public MangoTasks(TaskRepository taskRepository, ObjectMapper objectMapper, MangoSwarmProperties properties) {
        this.taskRepository = taskRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public UUID enqueue(String taskType, JsonNode payload) {
        return schedule(taskType, payload, Instant.now());
    }

    public UUID enqueue(String taskType, Object payload) {
        return schedule(taskType, payload, Instant.now());
    }

    public UUID scheduleAfter(String taskType, JsonNode payload, Duration delay) {
        Objects.requireNonNull(delay, "delay must not be null");
        return schedule(taskType, payload, Instant.now().plus(delay));
    }

    public UUID scheduleAfter(String taskType, Object payload, Duration delay) {
        Objects.requireNonNull(delay, "delay must not be null");
        return schedule(taskType, payload, Instant.now().plus(delay));
    }

    public UUID schedule(String taskType, JsonNode payload, Instant availableAt) {
        Objects.requireNonNull(taskType, "taskType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(availableAt, "availableAt must not be null");
        return taskRepository.enqueueInNextSlot(taskType, payload, availableAt, slotSpacing(taskType));
    }

    public UUID schedule(String taskType, Object payload, Instant availableAt) {
        Objects.requireNonNull(payload, "payload must not be null");
        return schedule(taskType, objectMapper.valueToTree(payload), availableAt);
    }

    private Duration slotSpacing(String taskType) {
        MangoSwarmProperties.TaskType config = properties.getTaskTypes().get(taskType);
        if (config == null) {
            throw new IllegalArgumentException("Task type is not configured: " + taskType);
        }
        if (config.getRate() <= 0) {
            return Duration.ZERO;
        }
        long nanos = Math.max(1L, config.getPeriod().toNanos() / config.getRate());
        return Duration.ofNanos(nanos);
    }
}
