package ie.bitstep.mango.swarm;

import java.time.Duration;
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
		UUID seriesId = UUID.randomUUID();
		Instant claimedAt = Instant.parse("2026-05-25T10:00:00Z");
		TaskExecutionContext<String> context = new TaskExecutionContext<>(
				taskId,
				"email",
				workerId,
				2,
				claimedAt,
				"payload",
				seriesId,
				(state, percent, message) -> {},
				(delay, payload) -> {
					throw new AssertionError("unexpected again() call");
				});

		assertThat(context.taskId()).isEqualTo(taskId);
		assertThat(context.taskType()).isEqualTo("email");
		assertThat(context.workerId()).isEqualTo(workerId);
		assertThat(context.attemptCount()).isEqualTo(2);
		assertThat(context.claimedAt()).isEqualTo(claimedAt);
		assertThat(context.payload()).isEqualTo("payload");
		assertThat(context.seriesId()).isEqualTo(seriesId);
	}

	@Test
	void seriesIdIsNullForStandaloneTasks() {
		TaskExecutionContext<String> context = context((state, percent, message) -> {});

		assertThat(context.seriesId()).isNull();
	}

	@Test
	void againDelegatesToAgainRequesterWithCurrentPayload() {
		List<Object[]> calls = new ArrayList<>();
		UUID resultId = UUID.randomUUID();
		TaskExecutionContext<String> context = new TaskExecutionContext<>(
				UUID.randomUUID(),
				"email",
				UUID.randomUUID(),
				1,
				Instant.parse("2026-05-25T10:00:00Z"),
				"payload",
				null,
				(state, percent, message) -> {},
				(delay, payload) -> {
					calls.add(new Object[] {delay, payload});
					return resultId;
				});

		UUID returned = context.again(Duration.ofMinutes(5));

		assertThat(returned).isEqualTo(resultId);
		assertThat(calls).hasSize(1);
		assertThat(calls.get(0)[0]).isEqualTo(Duration.ofMinutes(5));
		assertThat(calls.get(0)[1]).isEqualTo("payload");
	}

	@Test
	void againWithNewPayloadDelegatesWithOverriddenPayload() {
		List<Object[]> calls = new ArrayList<>();
		TaskExecutionContext<String> context = new TaskExecutionContext<>(
				UUID.randomUUID(),
				"email",
				UUID.randomUUID(),
				1,
				Instant.parse("2026-05-25T10:00:00Z"),
				"payload",
				null,
				(state, percent, message) -> {},
				(delay, payload) -> {
					calls.add(new Object[] {delay, payload});
					return UUID.randomUUID();
				});

		context.again(Duration.ofMinutes(1), "next-cursor");

		assertThat(calls).hasSize(1);
		assertThat(calls.get(0)[0]).isEqualTo(Duration.ofMinutes(1));
		assertThat(calls.get(0)[1]).isEqualTo("next-cursor");
	}

	@Test
	void againRejectsNullDelayAndPayload() {
		TaskExecutionContext<String> context = context((state, percent, message) -> {});

		assertThatNullPointerException()
				.isThrownBy(() -> context.again(null))
				.withMessage("delay must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> context.again(null, "payload"))
				.withMessage("delay must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> context.again(Duration.ofMinutes(1), null))
				.withMessage("newPayload must not be null");
	}

	@Test
	void reportsProgressWithAndWithoutDescription() {
		List<String> events = new ArrayList<>();
		TaskExecutionContext<String> context =
				context((state, percent, message) -> events.add(state + ":" + percent + ":" + message));

		context.progress(0, "started");
		context.progress(10);
		context.progress(100, "finished");

		assertThat(events).containsExactly("running:0:started", "running:10:null", "running:100:finished");
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
	void requiresIdentityProgressReporterAndAgainRequester() {
		UUID taskId = UUID.randomUUID();
		UUID workerId = UUID.randomUUID();
		Instant claimedAt = Instant.parse("2026-05-25T10:00:00Z");
		TaskExecutionContext.AgainRequester noopAgain = (delay, payload) -> null;

		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						null, "email", workerId, 1, claimedAt, "payload", null, (state, percent, message) -> {}, noopAgain))
				.withMessage("taskId must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						taskId, null, workerId, 1, claimedAt, "payload", null, (state, percent, message) -> {}, noopAgain))
				.withMessage("taskType must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						taskId, "email", null, 1, claimedAt, "payload", null, (state, percent, message) -> {}, noopAgain))
				.withMessage("workerId must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						taskId, "email", workerId, 1, claimedAt, "payload", null, null, noopAgain))
				.withMessage("progressReporter must not be null");
		assertThatNullPointerException()
				.isThrownBy(() -> new TaskExecutionContext<>(
						taskId, "email", workerId, 1, claimedAt, "payload", null, (state, percent, message) -> {}, null))
				.withMessage("againRequester must not be null");
	}

	private static TaskExecutionContext<String> context(TaskExecutionContext.ProgressReporter reporter) {
		return new TaskExecutionContext<>(
				UUID.randomUUID(),
				"email",
				UUID.randomUUID(),
				1,
				Instant.parse("2026-05-25T10:00:00Z"),
				"payload",
				null,
				reporter,
				(delay, payload) -> {
					throw new AssertionError("unexpected again() call");
				});
	}
}
