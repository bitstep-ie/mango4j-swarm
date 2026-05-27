package ie.bitstep.mango.swarm.handler;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskHandlerRegistryTest {

	@Test
	void rejectsDuplicateHandlers() {
		TaskHandler<String> first = new AnnotatedEmailHandler();
		TaskHandler<String> second = new AnotherAnnotatedEmailHandler();

		assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(first, second), Set.of("email"), false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Duplicate");
	}

	@Test
	void rejectsMissingConfiguredHandler() {
		assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(), Set.of("email"), false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("has no TaskHandler");
	}

	@Test
	void rejectsUnconfiguredHandlerByDefault() {
		assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(new AnnotatedEmailHandler()), Set.of(), false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("unconfigured");
	}

	@Test
	void acceptsAnnotatedHandlerWithoutTaskTypeMethod() {
		TaskHandler<?> handler = new AnnotatedEmailHandler();
		new TaskHandlerRegistry(List.of(handler), Set.of("email"), false);
	}

	@Test
	void exposesRegisteredTaskTypesAndHandlers() {
		TaskHandler<String> handler = new AnnotatedEmailHandler();
		TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(handler), Set.of("email"), false);

		assertThat(registry.taskTypes()).containsExactly("email");
		assertThat(registry.get("email")).isSameAs(handler);
		assertThatThrownBy(() -> registry.get("missing"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("No TaskHandler registered");
	}

	@Test
	void canAllowUnconfiguredHandlers() {
		TaskHandler<String> handler = new AnnotatedEmailHandler();
		TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(handler), Set.of(), true);

		assertThat(registry.taskTypes()).containsExactly("email");
	}

	@Test
	void rejectsHandlerWithoutAnnotation() {
		assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(new MissingTypeHandler()), Set.of("email"), false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("must declare task type");
	}

	@SwarmHandler("email")
	private static final class AnnotatedEmailHandler implements TaskHandler<String> {
		@Override
		public PayloadExtractor<String> payloadExtractor() {
			return reader -> "";
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			return TaskExecutionResult.completed();
		}
	}

	@SwarmHandler("email")
	private static final class AnotherAnnotatedEmailHandler implements TaskHandler<String> {
		@Override
		public PayloadExtractor<String> payloadExtractor() {
			return reader -> "";
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			return TaskExecutionResult.completed();
		}
	}

	private static final class MissingTypeHandler implements TaskHandler<String> {
		@Override
		public PayloadExtractor<String> payloadExtractor() {
			return reader -> "";
		}

		@Override
		public TaskExecutionResult execute(TaskExecutionContext<String> context) {
			return TaskExecutionResult.completed();
		}
	}
}
