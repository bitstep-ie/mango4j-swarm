package ie.bitstep.mango.swarm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * High-level task scheduling API for application code.
 * <p>
 * Tasks are persisted in PostgreSQL and then aligned to a task-type-specific slot spacing so
 * distributed workers can execute them under smooth rate limits.
 */
public class MangoTasks {
    private final TaskRepository taskRepository;
    private final ObjectMapper objectMapper;
    private final MangoSwarmProperties properties;

    public MangoTasks(TaskRepository taskRepository, ObjectMapper objectMapper, MangoSwarmProperties properties) {
        this.taskRepository = taskRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /**
     * Queues a task for immediate scheduling using a JSON payload.
     *
     * @param taskType configured task type key
     * @param payload JSON payload to store
     * @return persisted task id
     */
    public UUID queue(String taskType, JsonNode payload) {
        return at(Instant.now(), taskType, payload);
    }

    /**
     * Queues a task for immediate scheduling using an arbitrary object payload.
     *
     * @param taskType configured task type key
     * @param payload payload object converted via Jackson
     * @return persisted task id
     */
    public UUID queue(String taskType, Object payload) {
        return at(Instant.now(), taskType, payload);
    }

    /**
     * Schedules a task for execution after a delay using a JSON payload.
     *
     * @param delay delay from now
     * @param taskType configured task type key
     * @param payload JSON payload to store
     * @return persisted task id
     */
    public UUID after(Duration delay, String taskType, JsonNode payload) {
        Objects.requireNonNull(delay, "delay must not be null");
        return at(Instant.now().plus(delay), taskType, payload);
    }

    /**
     * Schedules a task for execution after a delay using an arbitrary object payload.
     *
     * @param delay delay from now
     * @param taskType configured task type key
     * @param payload payload object converted via Jackson
     * @return persisted task id
     */
    public UUID after(Duration delay, String taskType, Object payload) {
        Objects.requireNonNull(delay, "delay must not be null");
        return at(Instant.now().plus(delay), taskType, payload);
    }

    /**
     * Schedules a task at or after a requested instant using a JSON payload.
     *
     * @param at requested execution instant
     * @param taskType configured task type key
     * @param payload JSON payload to store
     * @return persisted task id
     */
    public UUID at(Instant at, String taskType, JsonNode payload) {
        Objects.requireNonNull(taskType, "taskType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(at, "at must not be null");
        return taskRepository.queueInNextSlot(taskType, payload, at, slotSpacing(taskType));
    }

    /**
     * Schedules a task at or after a requested instant using an arbitrary object payload.
     *
     * @param at requested execution instant
     * @param taskType configured task type key
     * @param payload payload object converted via Jackson
     * @return persisted task id
     */
    public UUID at(Instant at, String taskType, Object payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        return at(at, taskType, objectMapper.valueToTree(payload));
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
