package ie.bitstep.mango.swarm.executor;

import ie.bitstep.mango.swarm.config.MangoSwarmProperties;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

final class ExecutorFactory {
    private ExecutorFactory() {
    }

    static ExecutorService create(MangoSwarmProperties.Executor config) {
        boolean virtual = useVirtualThreads(config.getVirtualThreads());
        int maxThreads = resolveMaxThreads(config.getMaxThreads(), virtual);
        if (virtual) {
            ThreadFactory factory = virtualThreadFactory();
            if (factory != null) {
                ExecutorService service = newThreadPerTaskExecutor(factory);
                if (service != null) {
                    return service;
                }
            }
        }
        ThreadPoolExecutor.AbortPolicy abortPolicy = new ThreadPoolExecutor.AbortPolicy();
        return new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                30L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxThreads),
                Executors.defaultThreadFactory(),
                config.getQueueStrategy() == MangoSwarmProperties.QueueStrategy.ABORT
                        ? abortPolicy
                        : new ThreadPoolExecutor.CallerRunsPolicy());
    }

    static int resolveMaxThreads(String value, boolean virtualThreads) {
        if (value == null || "auto".equalsIgnoreCase(value)) {
            return virtualThreads ? 256 : 16;
        }
        return Integer.parseInt(value);
    }

    static boolean virtualThreadsAvailable() {
        return virtualThreadFactory() != null;
    }

    private static boolean useVirtualThreads(MangoSwarmProperties.VirtualThreads setting) {
        return switch (setting) {
            case ENABLED -> virtualThreadsAvailable();
            case DISABLED -> false;
            case AUTO -> virtualThreadsAvailable();
        };
    }

    private static ThreadFactory virtualThreadFactory() {
        try {
            Method ofVirtual = Thread.class.getMethod("ofVirtual");
            Object builder = ofVirtual.invoke(null);
            Method factory = builder.getClass().getMethod("factory");
            return (ThreadFactory) factory.invoke(builder);
        } catch (ReflectiveOperationException ex) {
            return null;
        }
    }

    private static ExecutorService newThreadPerTaskExecutor(ThreadFactory factory) {
        try {
            Method method = Executors.class.getMethod("newThreadPerTaskExecutor", ThreadFactory.class);
            return (ExecutorService) method.invoke(null, factory);
        } catch (ReflectiveOperationException ex) {
            return null;
        }
    }
}
