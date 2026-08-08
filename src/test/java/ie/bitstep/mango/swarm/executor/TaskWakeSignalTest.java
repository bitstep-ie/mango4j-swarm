package ie.bitstep.mango.swarm.executor;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TaskWakeSignalTest {

	@Test
	void awaitOrTimeoutIsNoOpForNullZeroAndNegativeDurations() {
		TaskWakeSignal signal = new TaskWakeSignal();
		assertThatCode(() -> {
					signal.awaitOrTimeout(null);
					signal.awaitOrTimeout(Duration.ZERO);
					signal.awaitOrTimeout(Duration.ofNanos(-1));
				})
				.doesNotThrowAnyException();
	}

	@Test
	void awaitOrTimeoutReturnsEarlyWhenSignaled() throws Exception {
		TaskWakeSignal signal = new TaskWakeSignal();
		CountDownLatch waiting = new CountDownLatch(1);
		AtomicLong elapsedNanos = new AtomicLong();
		Thread waiter = new Thread(() -> {
			waiting.countDown();
			long start = System.nanoTime();
			try {
				signal.awaitOrTimeout(Duration.ofSeconds(30));
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			elapsedNanos.set(System.nanoTime() - start);
		});
		waiter.start();
		assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(50);

		signal.signal();
		waiter.join(5_000);

		assertThat(waiter.isAlive()).isFalse();
		assertThat(Duration.ofNanos(elapsedNanos.get())).isLessThan(Duration.ofSeconds(5));
	}

	@Test
	void awaitOrTimeoutElapsesWhenNeverSignaled() throws Exception {
		TaskWakeSignal signal = new TaskWakeSignal();
		long start = System.nanoTime();

		signal.awaitOrTimeout(Duration.ofMillis(50));

		assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(Duration.ofMillis(40).toNanos());
	}

	@Test
	void pendingSignalIsConsumedByNextAwait() throws Exception {
		TaskWakeSignal signal = new TaskWakeSignal();
		signal.signal();
		long start = System.nanoTime();

		signal.awaitOrTimeout(Duration.ofSeconds(30));

		assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
	}
}
