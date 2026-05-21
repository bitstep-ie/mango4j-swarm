package ie.bitstep.mango.swarm.handler;

import ie.bitstep.mango.swarm.TaskExecutionContext;
import ie.bitstep.mango.swarm.TaskExecutionResult;
import ie.bitstep.mango.swarm.payload.PayloadExtractor;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskHandlerRegistryTest {

    @Test
    void rejectsDuplicateHandlers() {
        TaskHandler<String> first = handler("email");
        TaskHandler<String> second = handler("email");

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
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(handler("email")), Set.of(), false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unconfigured");
    }

    private static TaskHandler<String> handler(String type) {
        return new TaskHandler<>() {
            @Override
            public String taskType() {
                return type;
            }

            @Override
            public PayloadExtractor<String> payloadExtractor() {
                return reader -> "";
            }

            @Override
            public TaskExecutionResult execute(TaskExecutionContext<String> context) {
                return TaskExecutionResult.completed();
            }
        };
    }
}
