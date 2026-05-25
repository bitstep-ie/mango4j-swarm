package ie.bitstep.mango.swarm.executor;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutorFactoryTest {

	@Test
	void resolvesAutomaticAndExplicitThreadLimits() {
		assertThat(ExecutorFactory.resolveMaxThreads(null, false)).isEqualTo(16);
		assertThat(ExecutorFactory.resolveMaxThreads("auto", false)).isEqualTo(16);
		assertThat(ExecutorFactory.resolveMaxThreads("AUTO", true)).isEqualTo(256);
		assertThat(ExecutorFactory.resolveMaxThreads("7", false)).isEqualTo(7);
	}

	@Test
	void virtualThreadsAreUnavailableForJavaSeventeenBuild() {
		assertThat(ExecutorFactory.virtualThreadsAvailable()).isFalse();
	}

	@Test
	void namingFactoryPreservesNullDelegateThreads() throws Exception {
		ThreadFactory factory = (ThreadFactory) invokeStatic("namingFactory", (ThreadFactory) runnable -> null);

		assertThat(factory.newThread(() -> {})).isNull();
	}

	@Test
	void namesPlatformWorkerThreads() throws Exception {
		MangoSwarmProperties.Executor config = new MangoSwarmProperties.Executor();
		config.setVirtualThreads(MangoSwarmProperties.VirtualThreads.DISABLED);
		config.setMaxThreads("1");
		var executor = ExecutorFactory.create(config);

		String threadName =
				executor.submit(() -> Thread.currentThread().getName()).get(5, TimeUnit.SECONDS);
		executor.shutdownNow();

		assertThat(threadName).isEqualTo("swarm-worker-1");
	}

	@Test
	void abortStrategyRejectsWhenPoolAndQueueAreFull() throws Exception {
		MangoSwarmProperties.Executor config = new MangoSwarmProperties.Executor();
		config.setVirtualThreads(MangoSwarmProperties.VirtualThreads.DISABLED);
		config.setMaxThreads("1");
		config.setQueueStrategy(MangoSwarmProperties.QueueStrategy.ABORT);
		var executor = ExecutorFactory.create(config);
		CountDownLatch release = new CountDownLatch(1);

		executor.submit(() -> await(release));
		executor.submit(() -> {});

		assertThatThrownBy(() -> executor.submit(() -> {})).isInstanceOf(RejectedExecutionException.class);

		release.countDown();
		executor.shutdownNow();
	}

	@Test
	void callerRunsStrategyExecutesOverflowTaskInCallingThread() throws Exception {
		MangoSwarmProperties.Executor config = new MangoSwarmProperties.Executor();
		config.setVirtualThreads(MangoSwarmProperties.VirtualThreads.DISABLED);
		config.setMaxThreads("1");
		config.setQueueStrategy(MangoSwarmProperties.QueueStrategy.CALLER_RUNS);
		var executor = ExecutorFactory.create(config);
		CountDownLatch release = new CountDownLatch(1);

		executor.submit(() -> await(release));
		executor.submit(() -> {});
		String callerThread = Thread.currentThread().getName();
		String overflowThread =
				executor.submit(() -> Thread.currentThread().getName()).get(5, TimeUnit.SECONDS);

		release.countDown();
		executor.shutdownNow();
		assertThat(overflowThread).isEqualTo(callerThread);
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	private static Object invokeStatic(String name, Object... args) throws Exception {
		Class<?>[] parameterTypes = new Class<?>[args.length];
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof MangoSwarmProperties.VirtualThreads) {
				parameterTypes[i] = MangoSwarmProperties.VirtualThreads.class;
			} else if (args[i] instanceof ThreadFactory) {
				parameterTypes[i] = ThreadFactory.class;
			} else {
				parameterTypes[i] = args[i].getClass();
			}
		}
		Method method = ExecutorFactory.class.getDeclaredMethod(name, parameterTypes);
		method.setAccessible(true);
		return method.invoke(null, args);
	}
}
