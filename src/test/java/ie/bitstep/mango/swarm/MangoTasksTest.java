package ie.bitstep.mango.swarm;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import ie.bitstep.mango.swarm.db.TaskRecord;
import ie.bitstep.mango.swarm.db.TaskRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MangoTasksTest {

	@Test
	void queuesObjectPayloadForImmediateExecution() {
		RecordingRepository repository = new RecordingRepository();
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
		Instant before = Instant.now();

		UUID taskId = tasks.queue("email", new EmailRequest("customer-1", "x@example.com"));

		assertThat(taskId).isEqualTo(RecordingRepository.TASK_ID);
		assertThat(repository.taskType).isEqualTo("email");
		assertThat(repository.availableAt).isAfterOrEqualTo(before);
		assertThat(repository.payload.get("customerId").asText()).isEqualTo("customer-1");
		assertThat(repository.payload.get("email").asText()).isEqualTo("x@example.com");
	}

	@Test
	void queuesJsonPayloadForImmediateExecution() {
		RecordingRepository repository = new RecordingRepository();
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
		ObjectNode payload = JsonNodeFactory.instance
				.objectNode()
				.put("customerId", "customer-2")
				.put("email", "y@example.com");

		UUID taskId = tasks.queue("email", payload);

		assertThat(taskId).isEqualTo(RecordingRepository.TASK_ID);
		assertThat(repository.taskType).isEqualTo("email");
		assertThat(repository.payload).isSameAs(payload);
	}

	@Test
	void schedulesObjectPayloadAtRequestedTime() {
		RecordingRepository repository = new RecordingRepository();
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
		Instant availableAt = Instant.parse("2026-05-21T10:00:00Z");

		UUID taskId = tasks.at(availableAt, "email", new EmailRequest("customer-1", "x@example.com"));

		assertThat(taskId).isEqualTo(RecordingRepository.TASK_ID);
		assertThat(repository.taskType).isEqualTo("email");
		assertThat(repository.availableAt).isEqualTo(availableAt);
		assertThat(repository.payload.get("customerId").asText()).isEqualTo("customer-1");
		assertThat(repository.payload.get("email").asText()).isEqualTo("x@example.com");
	}

	@Test
	void scheduleAfterUsesFutureAvailableAt() {
		RecordingRepository repository = new RecordingRepository();
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
		Instant before = Instant.now().plusSeconds(29);

		UUID taskId = tasks.after(Duration.ofSeconds(30), "email", new EmailRequest("customer-1", "x@example.com"));

		assertThat(taskId).isEqualTo(RecordingRepository.TASK_ID);
		assertThat(repository.availableAt).isAfterOrEqualTo(before);
	}

	@Test
	void schedulesJsonPayloadAfterDelay() {
		RecordingRepository repository = new RecordingRepository();
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties());
		ObjectNode payload = JsonNodeFactory.instance.objectNode().put("customerId", "customer-3");
		Instant before = Instant.now().plusSeconds(4);

		UUID taskId = tasks.after(Duration.ofSeconds(5), "email", payload);

		assertThat(taskId).isEqualTo(RecordingRepository.TASK_ID);
		assertThat(repository.payload).isSameAs(payload);
		assertThat(repository.availableAt).isAfterOrEqualTo(before);
	}

	@Test
	void rejectsNullDelayTaskTypePayloadAndTime() {
		MangoTasks tasks = new MangoTasks(new RecordingRepository(), new ObjectMapper(), properties());
		ObjectNode payload = JsonNodeFactory.instance.objectNode();

		assertThatNullPointerException()
				.isThrownBy(() -> tasks.after(null, "email", payload))
				.withMessage("delay must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> tasks.after(null, "email", new EmailRequest("customer-1", "x@example.com")))
				.withMessage("delay must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> tasks.at(Instant.now(), null, payload))
				.withMessage("taskType must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> tasks.at(Instant.now(), "email", (JsonNode) null))
				.withMessage("payload must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> tasks.at(null, "email", payload))
				.withMessage("at must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> tasks.at(Instant.now(), "email", (Object) null))
				.withMessage("payload must not be null");
	}

	@Test
	void queuesSuccessfullyWhenRateIsNotPositive() {
		RecordingRepository repository = new RecordingRepository();
		MangoSwarmProperties properties = properties();
		properties.getTaskTypes().get("email").setRate(0);
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties);

		tasks.queue("email", JsonNodeFactory.instance.objectNode());

		assertThat(repository.taskType).isEqualTo("email");
	}

	@Test
	void rejectsUnconfiguredTaskType() {
		MangoTasks tasks = new MangoTasks(new RecordingRepository(), new ObjectMapper(), properties());

		assertThatThrownBy(() -> tasks.queue("unknown", JsonNodeFactory.instance.objectNode()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Task type is not configured: unknown");
	}

	@Test
	void rejectsNewTasksWhenTaskTypeModeRejectsQueueing() {
		RecordingRepository repository = new RecordingRepository();
		MangoSwarmProperties properties = properties();
		properties.getTaskTypes().get("email").setMode(MangoSwarmProperties.TaskMode.REJECT);
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties);
		ObjectNode payload = JsonNodeFactory.instance.objectNode();

		assertThatThrownBy(() -> tasks.queue("email", payload))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Task type is rejecting new tasks: email");
		assertThat(repository.taskType).isNull();
	}

	@Test
	void dropsNewTasksWithoutWritingWhenTaskTypeModeDropsQueueing() {
		RecordingRepository repository = new RecordingRepository();
		MangoSwarmProperties properties = properties();
		properties.getTaskTypes().get("email").setMode(MangoSwarmProperties.TaskMode.DROP);
		MangoTasks tasks = new MangoTasks(repository, new ObjectMapper(), properties);

		UUID taskId = tasks.queue("email", JsonNodeFactory.instance.objectNode());

		assertThat(taskId).isNotNull();
		assertThat(repository.taskType).isNull();
	}

	private static MangoSwarmProperties properties() {
		MangoSwarmProperties properties = new MangoSwarmProperties();
		MangoSwarmProperties.TaskType email = new MangoSwarmProperties.TaskType();
		email.setRate(100);
		email.setPeriod(Duration.ofSeconds(1));
		properties.getTaskTypes().put("email", email);
		return properties;
	}

	private static final class EmailRequest {
		private final String customerId;
		private final String email;

		private EmailRequest(String customerId, String email) {
			this.customerId = customerId;
			this.email = email;
		}

		public String getCustomerId() {
			return customerId;
		}

		public String getEmail() {
			return email;
		}
	}

	private static final class RecordingRepository implements TaskRepository {
		private String taskType;
		private JsonNode payload;
		private Instant availableAt;

		private static final UUID TASK_ID = UUID.fromString("018f0000-0000-7000-8000-000000000001");

		@Override
		public UUID queue(String taskType, JsonNode payload, Instant availableAt) {
			this.taskType = taskType;
			this.payload = payload;
			this.availableAt = availableAt;
			return TASK_ID;
		}

		@Override
		public List<TaskRecord> claimBatch(String taskType, UUID workerId, Instant now, int limit) {
			return List.of();
		}

		@Override
		public void markInProgress(UUID taskId, UUID workerId, Instant now) {}

		@Override
		public void updateRuntime(
				UUID taskId,
				UUID workerId,
				Instant now,
				String executionState,
				Integer progressPercent,
				String message) {}

		@Override
		public void recordProgress(UUID taskId, UUID workerId, Instant now, int progressPercent, String description) {}

		@Override
		public void markCompleted(UUID taskId, UUID workerId, Instant now) {}

		@Override
		public void markFailed(UUID taskId, UUID workerId, Instant now, String errorMessage) {}

		@Override
		public void rescheduleAfterFailure(
				UUID taskId, UUID workerId, Instant now, Instant availableAt, String errorMessage) {}

		@Override
		public void requeueClaimed(UUID taskId, UUID workerId, Instant now, Instant availableAt, String reason) {}

		@Override
		public int reclaimTimedOut(String taskType, Duration timeout, Instant now, int limit) {
			return 0;
		}

		@Override
		public int markTimedOutFailed(String taskType, Duration timeout, Instant now, int limit) {
			return 0;
		}

		@Override
		public int deleteCompletedOlderThan(Duration retention, Instant now, int limit) {
			return 0;
		}

		@Override
		public int deleteFailedOlderThan(Duration retention, Instant now, int limit) {
			return 0;
		}
	}
}
