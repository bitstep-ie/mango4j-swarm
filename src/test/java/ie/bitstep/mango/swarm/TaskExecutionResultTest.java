package ie.bitstep.mango.swarm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskExecutionResultTest {

	@Test
	void completedResultIsSingleton() {
		assertThat(TaskExecutionResult.completed()).isSameAs(TaskExecutionResult.completed());
	}

	@Test
	void failedResultCarriesMessage() {
		TaskExecutionResult.Failed failed = TaskExecutionResult.failed("boom");

		assertThat(failed.message()).isEqualTo("boom");
	}
}
