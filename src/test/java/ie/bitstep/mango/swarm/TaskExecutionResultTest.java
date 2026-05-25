package ie.bitstep.mango.swarm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskExecutionResultTest {

    @Test
    void completedResultIsSuccessfulSingleton() {
        TaskExecutionResult completed = TaskExecutionResult.completed();

        assertThat(completed).isSameAs(TaskExecutionResult.completed());
        assertThat(completed.isCompleted()).isTrue();
        assertThat(completed.message()).isNull();
    }

    @Test
    void failedResultCarriesMessage() {
        TaskExecutionResult failed = TaskExecutionResult.failed("boom");

        assertThat(failed.isCompleted()).isFalse();
        assertThat(failed.message()).isEqualTo("boom");
    }
}
