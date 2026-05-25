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

	private ExecutorFactory() {}

	static ExecutorService create(MangoSwarmProperties.Executor config) {
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

	static int resolveMaxThreads(String value, boolean virtualThreads) {
		if (value == null || "auto".equalsIgnoreCase(value)) {
			return virtualThreads ? 256 : 16;
		}
		return Integer.parseInt(value);
	}

	static boolean virtualThreadsAvailable() {
		return false;
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
