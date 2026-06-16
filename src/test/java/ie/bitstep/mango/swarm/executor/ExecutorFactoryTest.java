package ie.bitstep.mango.swarm.executor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import ie.bitstep.mango.swarm.config.MangoSwarmProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ExecutorFactoryTest {

	@Test
	void resolvesAutomaticAndExplicitThreadLimits() {
		assertThat(ExecutorFactory.resolveMaxThreads(null, false)).isEqualTo(16);
		assertThat(ExecutorFactory.resolveMaxThreads("auto", false)).isEqualTo(16);
		assertThat(ExecutorFactory.resolveMaxThreads("AUTO", true)).isEqualTo(256);
		assertThat(ExecutorFactory.resolveMaxThreads("7", false)).isEqualTo(7);
	}

	@Test
	void clampsThreadLimitsWithinSupportedBounds() {
		assertThat(ExecutorFactory.resolveMaxThreads("0", false)).isEqualTo(1);
		assertThat(ExecutorFactory.resolveMaxThreads("9999", false)).isEqualTo(256);
		assertThat(ExecutorFactory.resolveMaxThreads(" 12 ", false)).isEqualTo(12);
	}

	@Test
	void rejectsNonNumericThreadLimits() {
		assertThatThrownBy(() -> ExecutorFactory.resolveMaxThreads("many", false))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("mango4j.swarm.executor.maxThreads");
	}

	@Test
	void virtualThreadsAreUnavailableForJavaSeventeenBuild() {
		assumeTrue(Runtime.version().feature() < 21, "only applies to Java < 21");
		assertThat(ExecutorFactory.virtualThreadsAvailable()).isFalse();
	}

	@Test
	void utilityConstructorRejectsInstances() throws Exception {
		Constructor<ExecutorFactory> constructor = ExecutorFactory.class.getDeclaredConstructor();
		constructor.setAccessible(true);

		assertThatThrownBy(constructor::newInstance)
				.isInstanceOf(InvocationTargetException.class)
				.extracting(Throwable::getCause)
				.satisfies(cause ->
						assertThat(cause).isInstanceOf(AssertionError.class).hasMessage("No instances"));
	}

	@Test
	void namingFactoryPreservesNullDelegateThreads() throws Exception {
		ThreadFactory factory = (ThreadFactory) invokeStatic("namingFactory", (ThreadFactory) runnable -> null);

		assertThat(factory.newThread(() -> {})).isNull();
	}

	@Test
	void namingFactoryAllowsNullDelegate() throws Exception {
		assertThat(invokeStatic("namingFactory", new Object[] {null})).isNull();
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
	void usesVirtualExecutorFactoryWhenAvailableAndEnabled() {
		MangoSwarmProperties.Executor config = new MangoSwarmProperties.Executor();
		var executor = Executors.newSingleThreadExecutor();
		try {
			assertThat(ExecutorFactory.create(config, true, () -> executor)).isSameAs(executor);
		} finally {
			executor.shutdownNow();
		}
	}

	@Test
	void abortStrategyRejectsWhenPoolAndQueueAreFull() {
		MangoSwarmProperties.Executor config = new MangoSwarmProperties.Executor();
		config.setVirtualThreads(MangoSwarmProperties.VirtualThreads.DISABLED);
		config.setMaxThreads("1");
		config.setQueueStrategy(MangoSwarmProperties.QueueStrategy.ABORT);
		var executor = ExecutorFactory.create(config);
		CountDownLatch release = new CountDownLatch(1);
		try {
			executor.submit(() -> await(release));
			executor.submit(() -> {});

			assertThatThrownBy(() -> executor.submit(() -> {})).isInstanceOf(RejectedExecutionException.class);
		} finally {
			release.countDown();
			executor.shutdownNow();
		}
	}

	@Test
	void callerRunsStrategyExecutesOverflowTaskInCallingThread() throws Exception {
		MangoSwarmProperties.Executor config = new MangoSwarmProperties.Executor();
		config.setVirtualThreads(MangoSwarmProperties.VirtualThreads.DISABLED);
		config.setMaxThreads("1");
		config.setQueueStrategy(MangoSwarmProperties.QueueStrategy.CALLER_RUNS);
		var executor = ExecutorFactory.create(config);
		CountDownLatch release = new CountDownLatch(1);
		String callerThread = Thread.currentThread().getName();
		try {
			executor.submit(() -> await(release));
			executor.submit(() -> {});
			String overflowThread =
					executor.submit(() -> Thread.currentThread().getName()).get(5, TimeUnit.SECONDS);

			assertThat(overflowThread).isEqualTo(callerThread);
		} finally {
			release.countDown();
			executor.shutdownNow();
		}
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
			if (args[i] == null) {
				parameterTypes[i] = ThreadFactory.class;
			} else if (args[i] instanceof MangoSwarmProperties.VirtualThreads) {
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
