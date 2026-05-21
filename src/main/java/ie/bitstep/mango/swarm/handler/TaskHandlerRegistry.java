package ie.bitstep.mango.swarm.handler;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class TaskHandlerRegistry {
    private final Map<String, TaskHandler<?>> handlers;

    public TaskHandlerRegistry(Collection<TaskHandler<?>> handlers, Set<String> configuredTypes, boolean allowUnconfiguredHandlers) {
        Map<String, TaskHandler<?>> discovered = new LinkedHashMap<>();
        for (TaskHandler<?> handler : handlers) {
            String type = Objects.requireNonNull(handler.taskType(), "taskType");
            TaskHandler<?> previous = discovered.putIfAbsent(type, handler);
            if (previous != null) {
                throw new IllegalStateException("Duplicate TaskHandler for task type '" + type + "'");
            }
        }
        for (String configuredType : configuredTypes) {
            if (!discovered.containsKey(configuredType)) {
                throw new IllegalStateException("Configured task type '" + configuredType + "' has no TaskHandler");
            }
        }
        if (!allowUnconfiguredHandlers) {
            for (String handlerType : discovered.keySet()) {
                if (!configuredTypes.contains(handlerType)) {
                    throw new IllegalStateException("TaskHandler exists for unconfigured task type '" + handlerType + "'");
                }
            }
        }
        this.handlers = Map.copyOf(discovered);
    }

    public TaskHandler<?> get(String taskType) {
        TaskHandler<?> handler = handlers.get(taskType);
        if (handler == null) {
            throw new IllegalArgumentException("No TaskHandler registered for task type '" + taskType + "'");
        }
        return handler;
    }

    public Set<String> taskTypes() {
        return handlers.keySet();
    }
}
