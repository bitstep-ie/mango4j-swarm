package ie.bitstep.mango.swarm.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import ie.bitstep.mango.swarm.config.MangoSwarmProperties;

final class ExecutorFactory {
	private static final String WORKER_THREAD_PREFIX = "swarm-worker-";
	private static final boolean VIRTUAL_THREADS_AVAILABLE;

	static {
		boolean available;
		try {
			Thread.class.getMethod("ofVirtual");
			Executors.class.getMethod("newThreadPerTaskExecutor", ThreadFactory.class);
			available = true;
		} catch (NoSuchMethodException ignored) {
			available = false;
		}
		VIRTUAL_THREADS_AVAILABLE = available;
	}

	private ExecutorFactory() {
		throw new AssertionError("No instances");
	}

	static ExecutorService create(MangoSwarmProperties.Executor config) {
		boolean virtual =
				config.getVirtualThreads() != MangoSwarmProperties.VirtualThreads.DISABLED && VIRTUAL_THREADS_AVAILABLE;
		if (virtual) {
			return createVirtualThreadExecutor();
		}
		int maxThreads = resolveMaxThreads(config.getMaxThreads(), false);
		ThreadPoolExecutor.AbortPolicy abortPolicy = new ThreadPoolExecutor.AbortPolicy();
		return new ThreadPoolExecutor(
				maxThreads,
				maxThreads,
				30L,
				TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(maxThreads),
				namingFactory(Executors.defaultThreadFactory()),
				config.getQueueStrategy() == MangoSwarmProperties.QueueStrategy.ABORT
						? abortPolicy
						: new ThreadPoolExecutor.CallerRunsPolicy());
	}

	private static ExecutorService createVirtualThreadExecutor() {
		try {
			Class<?> builderType = Class.forName("java.lang.Thread$Builder");
			Object builder = Thread.class.getMethod("ofVirtual").invoke(null);
			builder = builderType.getMethod("name", String.class, long.class).invoke(builder, WORKER_THREAD_PREFIX, 1L);
			ThreadFactory factory =
					(ThreadFactory) builderType.getMethod("factory").invoke(builder);
			return (ExecutorService) Executors.class
					.getMethod("newThreadPerTaskExecutor", ThreadFactory.class)
					.invoke(null, factory);
		} catch (ReflectiveOperationException ex) {
			throw new IllegalStateException("Virtual thread executor creation failed", ex);
		}
	}

	static int resolveMaxThreads(String value, boolean virtualThreads) {
		if (value == null || "auto".equalsIgnoreCase(value)) {
			return virtualThreads ? 256 : 16;
		}
		return Integer.parseInt(value);
	}

	static boolean virtualThreadsAvailable() {
		return VIRTUAL_THREADS_AVAILABLE;
	}

	private static ThreadFactory namingFactory(ThreadFactory delegate) {
		if (delegate == null) {
			return null;
		}
		AtomicLong sequence = new AtomicLong(0);
		return runnable -> {
			Thread thread = delegate.newThread(runnable);
			if (thread != null) {
				thread.setName(WORKER_THREAD_PREFIX + sequence.incrementAndGet());
			}
			return thread;
		};
	}
}
