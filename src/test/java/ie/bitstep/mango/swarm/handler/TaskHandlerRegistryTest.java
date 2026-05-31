package ie.bitstep.mango.swarm.handler;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskHandlerRegistryTest {

	@Test
	void rejectsDuplicateHandlers() {
		TaskHandler<String> first = new AnnotatedEmailHandler();
		TaskHandler<String> second = new AnotherAnnotatedEmailHandler();
		List<TaskHandler<?>> handlers = List.of(first, second);
		Set<String> configuredTypes = Set.of("email");

		assertThatThrownBy(() -> new TaskHandlerRegistry(handlers, configuredTypes, false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Duplicate");
	}

	@Test
	void rejectsMissingConfiguredHandler() {
		List<TaskHandler<?>> handlers = List.of();
		Set<String> configuredTypes = Set.of("email");

		assertThatThrownBy(() -> new TaskHandlerRegistry(handlers, configuredTypes, false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("has no TaskHandler");
	}

	@Test
	void rejectsUnconfiguredHandlerByDefault() {
		List<TaskHandler<?>> handlers = List.of(new AnnotatedEmailHandler());
		Set<String> configuredTypes = Set.of();

		assertThatThrownBy(() -> new TaskHandlerRegistry(handlers, configuredTypes, false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("unconfigured");
	}

	@Test
	void acceptsAnnotatedHandlerWithoutTaskTypeMethod() {
		TaskHandler<?> handler = new AnnotatedEmailHandler();

		assertThatCode(() -> new TaskHandlerRegistry(List.of(handler), Set.of("email"), false))
				.doesNotThrowAnyException();
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
		List<TaskHandler<?>> handlers = List.of(new MissingTypeHandler());
		Set<String> configuredTypes = Set.of("email");

		assertThatThrownBy(() -> new TaskHandlerRegistry(handlers, configuredTypes, false))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("must declare task type");
	}

	@Test
	void rejectsHandlerWithBlankAnnotationValue() {
		List<TaskHandler<?>> handlers = List.of(new BlankTypeHandler());
		Set<String> configuredTypes = Set.of();

		assertThatThrownBy(() -> new TaskHandlerRegistry(handlers, configuredTypes, true))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("must not be blank");
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

	@SwarmHandler(" ")
	private static final class BlankTypeHandler implements TaskHandler<String> {
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
