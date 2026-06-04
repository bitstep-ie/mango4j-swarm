package ie.bitstep.mango.swarm.rate;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Fixed-size, object-reusing token ring for local per-worker task start pacing. */
public final class LocalTokenRingRateLimiter {
	private final Token[] tokens;
	private int head;
	private int activeTokens;
	private Duration spacing = Duration.ofSeconds(1);
	private boolean disabled = true;
	private boolean initialized;

	public LocalTokenRingRateLimiter(int capacity) {
		if (capacity < 1) {
			throw new IllegalArgumentException("capacity must be at least 1");
		}
		this.tokens = new Token[capacity];
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = new Token();
		}
		this.activeTokens = tokens.length;
	}

	public synchronized void configure(double rate, Duration period, Instant now) {
		Objects.requireNonNull(period, "period must not be null");
		Objects.requireNonNull(now, "now must not be null");
		if (rate <= 0.0d || period.isNegative() || period.isZero()) {
			disabled = true;
			return;
		}
		long nanos = Math.max(1L, (long) (period.toNanos() / rate));
		int configuredActiveTokens = Math.max(1, Math.min(tokens.length, (int) Math.ceil(rate)));
		spacing = Duration.ofNanos(nanos);
		disabled = false;
		if (!initialized || configuredActiveTokens != activeTokens) {
			activeTokens = configuredActiveTokens;
			initialize(now);
		} else {
			activeTokens = configuredActiveTokens;
		}
	}

	public synchronized Acquisition acquire(Instant now) {
		Objects.requireNonNull(now, "now must not be null");
		if (disabled) {
			return Acquisition.deny();
		}
		while (true) {
			Token token = tokens[head];
			if (token.availableAt.isAfter(now)) {
				return Acquisition.waitFor(Duration.between(now, token.availableAt));
			}
			if (isExpired(token, now)) {
				recycle(token, now);
				advanceHead();
				continue;
			}
			recycle(token, now);
			advanceHead();
			return Acquisition.grant();
		}
	}

	public synchronized int availablePermits(Instant now, int max) {
		Objects.requireNonNull(now, "now must not be null");
		if (max <= 0 || disabled) {
			return 0;
		}
		skipExpired(now);
		int available = 0;
		for (int offset = 0; offset < activeTokens && available < max; offset++) {
			Token token = tokens[index(offset)];
			if (token.availableAt.isAfter(now) || isExpired(token, now)) {
				break;
			}
			available++;
		}
		return available;
	}

	public synchronized Duration timeUntilNextPermit(Instant now) {
		Objects.requireNonNull(now, "now must not be null");
		if (disabled) {
			return Duration.ZERO;
		}
		skipExpired(now);
		Token token = tokens[head];
		return token.availableAt.isAfter(now) ? Duration.between(now, token.availableAt) : Duration.ZERO;
	}

	Token headToken() {
		return tokens[head];
	}

	Token tokenAt(int index) {
		return tokens[index];
	}

	private void initialize(Instant now) {
		head = 0;
		for (int i = 0; i < tokens.length; i++) {
			if (i < activeTokens) {
				tokens[i].schedule(now.plus(spacing.multipliedBy(i)), spacing);
			} else {
				tokens[i].disable();
			}
		}
		initialized = true;
	}

	private void skipExpired(Instant now) {
		while (isExpired(tokens[head], now)) {
			recycle(tokens[head], now);
			advanceHead();
		}
	}

	private boolean isExpired(Token token, Instant now) {
		return !token.expiresAt.isAfter(now);
	}

	private void recycle(Token token, Instant now) {
		Instant anchor = latestScheduledToken();
		if (anchor.isBefore(now)) {
			anchor = now;
		}
		token.schedule(anchor.plus(spacing), spacing);
	}

	private Instant latestScheduledToken() {
		Instant latest = Instant.EPOCH;
		for (int i = 0; i < activeTokens; i++) {
			Instant availableAt = tokens[i].availableAt;
			if (availableAt.isAfter(latest)) {
				latest = availableAt;
			}
		}
		return latest;
	}

	private void advanceHead() {
		head = index(1);
	}

	private int index(int offset) {
		return (head + offset) % activeTokens;
	}

	static final class Token {
		private Instant availableAt = Instant.MAX;
		private Instant expiresAt = Instant.MAX;

		Instant availableAt() {
			return availableAt;
		}

		Instant expiresAt() {
			return expiresAt;
		}

		private void schedule(Instant availableAt, Duration ttl) {
			this.availableAt = availableAt;
			this.expiresAt = availableAt.plus(ttl);
		}

		private void disable() {
			this.availableAt = Instant.MAX;
			this.expiresAt = Instant.MAX;
		}
	}

	public record Acquisition(boolean granted, Duration waitDuration) {
		private static Acquisition grant() {
			return new Acquisition(true, Duration.ZERO);
		}

		private static Acquisition waitFor(Duration waitDuration) {
			return new Acquisition(false, waitDuration);
		}

		private static Acquisition deny() {
			return new Acquisition(false, Duration.ZERO);
		}
	}
}
