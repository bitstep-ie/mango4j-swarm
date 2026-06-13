package ie.bitstep.mango.swarm;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRepository;

/**
 * High-level task scheduling API for application code.
 *
 * <p>Tasks are persisted in PostgreSQL with an earliest eligibility time. Local worker token rings decide when an
 * eligible task may actually start.
 */
public class MangoTasks {
	private final TaskRepository taskRepository;
	private final ObjectMapper objectMapper;
	private final MangoSwarmProperties properties;
	private final Clock clock;

	public MangoTasks(
			TaskRepository taskRepository, ObjectMapper objectMapper, MangoSwarmProperties properties, Clock clock) {
		this.taskRepository = taskRepository;
		this.objectMapper = objectMapper;
		this.properties = properties;
		this.clock = clock;
	}

	/**
	 * Queues a task for immediate scheduling using a JSON payload.
	 *
	 * @param taskType configured task type key
	 * @param payload JSON payload to store
	 * @return persisted task id
	 */
	public UUID queue(String taskType, JsonNode payload) {
		return at(Instant.now(clock), taskType, payload);
	}

	/**
	 * Queues a task for immediate scheduling using an arbitrary object payload.
	 *
	 * @param taskType configured task type key
	 * @param payload payload object converted via Jackson
	 * @return persisted task id
	 */
	public UUID queue(String taskType, Object payload) {
		return at(Instant.now(clock), taskType, payload);
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
		return at(Instant.now(clock).plus(delay), taskType, payload);
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
		return at(Instant.now(clock).plus(delay), taskType, payload);
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
		MangoSwarmProperties.TaskType config = taskTypeConfig(taskType);
		if (config.getMode() == MangoSwarmProperties.TaskMode.REJECT) {
			throw new IllegalStateException("Task type is rejecting new tasks: " + taskType);
		}
		if (config.getMode() == MangoSwarmProperties.TaskMode.DROP) {
			return UuidV7.generate();
		}
		return taskRepository.queue(taskType, payload, at);
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

	private MangoSwarmProperties.TaskType taskTypeConfig(String taskType) {
		MangoSwarmProperties.TaskType config = properties.getTaskTypes().get(taskType);
		if (config == null) {
			throw new IllegalArgumentException("Task type is not configured: " + taskType);
		}
		return config;
	}
}
