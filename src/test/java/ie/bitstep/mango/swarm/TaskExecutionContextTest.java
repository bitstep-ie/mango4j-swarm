package ie.bitstep.mango.swarm;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskExecutionContextTest {

	@Test
	void exposesExecutionMetadataAndPayload() {
		UUID taskId = UUID.randomUUID();
		UUID workerId = UUID.randomUUID();
		Instant claimedAt = Instant.parse("2026-05-25T10:00:00Z");
		TaskExecutionContext<String> context = new TaskExecutionContext<>(
				taskId, "email", workerId, 2, claimedAt, "payload", (state, percent, message) -> {});

		assertThat(context.taskId()).isEqualTo(taskId);
		assertThat(context.taskType()).isEqualTo("email");
		assertThat(context.workerId()).isEqualTo(workerId);
		assertThat(context.attemptCount()).isEqualTo(2);
		assertThat(context.claimedAt()).isEqualTo(claimedAt);
		assertThat(context.payload()).isEqualTo("payload");
	}

	@Test
	void reportsProgressWithAndWithoutDescription() {
		List<String> events = new ArrayList<>();
		TaskExecutionContext<String> context =
				context((state, percent, message) -> events.add(state + ":" + percent + ":" + message));

		context.progress(0, "started");
		context.progress(10);
		context.progress(100, "finished");
		context.updateState("Calling partner API");

		assertThat(events)
				.containsExactly(
						"running:0:started",
						"running:10:null",
						"running:100:finished",
						"Calling partner API:null:null");
	}

	@Test
	void rejectsProgressOutsidePercentageBounds() {
		TaskExecutionContext<String> context = context((state, percent, message) -> {});

		assertThatThrownBy(() -> context.progress(-1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("between 0 and 100");
		assertThatThrownBy(() -> context.progress(101))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("between 0 and 100");
	}

	@Test
	void requiresIdentityAndProgressReporter() {
		UUID taskId = UUID.randomUUID();
		UUID workerId = UUID.randomUUID();
		Instant claimedAt = Instant.parse("2026-05-25T10:00:00Z");

		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						null, "email", workerId, 1, claimedAt, "payload", (state, percent, message) -> {}))
				.withMessage("taskId must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						taskId, null, workerId, 1, claimedAt, "payload", (state, percent, message) -> {}))
				.withMessage("taskType must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						taskId, "email", null, 1, claimedAt, "payload", (state, percent, message) -> {}))
				.withMessage("workerId must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(taskId, "email", workerId, 1, claimedAt, "payload", null))
				.withMessage("progressReporter must not be null");
	}

	private static TaskExecutionContext<String> context(TaskExecutionContext.ProgressReporter reporter) {
		return new TaskExecutionContext<>(
				UUID.randomUUID(),
				"email",
				UUID.randomUUID(),
				1,
				Instant.parse("2026-05-25T10:00:00Z"),
				"payload",
				reporter);
	}
}
