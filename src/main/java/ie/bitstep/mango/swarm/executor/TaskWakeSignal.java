package ie.bitstep.mango.swarm.executor;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Coalesced wake signal that lets task submission interrupt the daemon's empty-queue backoff early.
 *
 * <p>{@link #signal()} is safe to call from any thread and never blocks. A pending signal is consumed by the next
 * {@link #awaitOrTimeout(Duration)} call; repeated signals before that call are coalesced into one wake-up.
 */
public final class TaskWakeSignal {
	private static final Object TOKEN = new Object();
	private final ArrayBlockingQueue<Object> pending = new ArrayBlockingQueue<>(1);

	public void signal() {
		pending.offer(TOKEN);
	}

	public void awaitOrTimeout(Duration timeout) throws InterruptedException {
		if (timeout == null || timeout.isNegative() || timeout.isZero()) {
			return;
		}
		pending.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}
}
