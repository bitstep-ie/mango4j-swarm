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
				taskId, "email", workerId, 2, claimedAt, "payload", (percent, description) -> {});

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
				context((percent, description) -> events.add(percent + ":" + description));

		context.progress(0, "started");
		context.progress(10);
		context.progress(100, "finished");

		assertThat(events).containsExactly("0:started", "10:null", "100:finished");
	}

	@Test
	void rejectsProgressOutsidePercentageBounds() {
		TaskExecutionContext<String> context = context((percent, description) -> {});

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
				.isThrownBy(() ->
						new TaskExecutionContext<>(null, "email", workerId, 1, claimedAt, "payload", (p, d) -> {}))
				.withMessage("taskId must not be null");
		assertThatNullPointerException()
				.isThrownBy(
						() -> new TaskExecutionContext<>(taskId, null, workerId, 1, claimedAt, "payload", (p, d) -> {}))
				.withMessage("taskType must not be null");
		assertThatNullPointerException()
				.isThrownBy(
						() -> new TaskExecutionContext<>(taskId, "email", null, 1, claimedAt, "payload", (p, d) -> {}))
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
