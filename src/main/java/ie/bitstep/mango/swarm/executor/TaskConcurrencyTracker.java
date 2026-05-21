package ie.bitstep.mango.swarm.executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

final class TaskConcurrencyTracker {
    private final Map<String, Semaphore> semaphores = new ConcurrentHashMap<>();
    private final Map<String, Integer> limits;

    TaskConcurrencyTracker(Map<String, Integer> limits) {
        this.limits = Map.copyOf(limits);
    }

    int remaining(String taskType) {
        return semaphore(taskType).availablePermits();
    }

    boolean tryAcquire(String taskType) {
        return semaphore(taskType).tryAcquire();
    }

    void release(String taskType) {
        semaphore(taskType).release();
    }

    private Semaphore semaphore(String taskType) {
        return semaphores.computeIfAbsent(taskType, type -> new Semaphore(limits.getOrDefault(type, 1)));
    }
}
