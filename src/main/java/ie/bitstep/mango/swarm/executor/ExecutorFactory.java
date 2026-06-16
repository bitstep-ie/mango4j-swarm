package ie.bitstep.mango.swarm.executor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import ie.bitstep.mango.swarm.config.MangoSwarmProperties;

final class ExecutorFactory {
	private static final String WORKER_THREAD_PREFIX = "swarm-worker-";
	private static final int MIN_THREADS = 1;
	private static final int MAX_THREADS = 256;
	private static final boolean VIRTUAL_THREADS_AVAILABLE;

	static {
		VIRTUAL_THREADS_AVAILABLE = detectVirtualThreadsAvailable();
	}

	private ExecutorFactory() {
		throw new AssertionError("No instances");
	}

	@Generated
	private static boolean detectVirtualThreadsAvailable() {
		boolean available;
		try {
			Class<?> builderType = Class.forName("java.lang.Thread$Builder");
			Thread.class.getMethod("ofVirtual");
			builderType.getMethod("name", String.class, long.class);
			builderType.getMethod("factory");
			Executors.class.getMethod("newThreadPerTaskExecutor", ThreadFactory.class);
			available = true;
		} catch (ClassNotFoundException | NoSuchMethodException ignored) {
			available = false;
		}
		return available;
	}

	static ExecutorService create(MangoSwarmProperties.Executor config) {
		return create(config, VIRTUAL_THREADS_AVAILABLE, ExecutorFactory::createVirtualThreadExecutor);
	}

	static ExecutorService create(
			MangoSwarmProperties.Executor config,
			boolean virtualThreadsAvailable,
			Supplier<ExecutorService> virtualExecutor) {
		boolean virtual =
				config.getVirtualThreads() != MangoSwarmProperties.VirtualThreads.DISABLED && virtualThreadsAvailable;
		if (virtual) {
			return virtualExecutor.get();
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

	@Generated
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
		if (value == null) {
			return virtualThreads ? MAX_THREADS : 16;
		}
		String normalized = value.trim();
		if (normalized.isEmpty() || "auto".equalsIgnoreCase(normalized)) {
			return virtualThreads ? MAX_THREADS : 16;
		}
		int parsed;
		try {
			parsed = Integer.parseInt(normalized);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(
					"mango4j.swarm.executor.maxThreads must be numeric or auto: " + value, ex);
		}
		return Math.max(MIN_THREADS, Math.min(MAX_THREADS, parsed));
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

	@Retention(RetentionPolicy.CLASS)
	@Target({ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE})
	private @interface Generated {}
}
