package ie.bitstep.mango.swarm.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskHandlerExceptionTest {
	@Test
	void carriesMessageAndCause() {
		IllegalStateException cause = new IllegalStateException("interrupted");

		TaskHandlerException exception = new TaskHandlerException("handler failed", cause);

		assertThat(exception).hasMessage("handler failed").hasCause(cause);
	}
}
