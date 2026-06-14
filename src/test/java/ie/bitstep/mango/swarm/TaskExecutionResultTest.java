package ie.bitstep.mango.swarm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskExecutionResultTest {

	@Test
	void completedReturnsNonNullInstance() {
		assertThat(TaskExecutionResult.completed()).isNotNull().isInstanceOf(TaskExecutionResult.Completed.class);
	}

	@Test
	void completedResultIsEqualToAnotherCompleted() {
		assertThat(TaskExecutionResult.completed()).isEqualTo(TaskExecutionResult.completed());
	}

	@Test
	void failedResultCarriesMessage() {
		TaskExecutionResult.Failed failed = TaskExecutionResult.failed("boom");

		assertThat(failed.message()).isEqualTo("boom");
	}
}
