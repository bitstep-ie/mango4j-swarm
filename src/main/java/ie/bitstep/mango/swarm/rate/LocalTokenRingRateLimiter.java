package ie.bitstep.mango.swarm.rate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Objects;

/** Fixed-size, object-reusing token ring for local per-worker task start pacing. */
public final class LocalTokenRingRateLimiter {
	private static final String NOW_REQUIRED = "now must not be null";

	private final Token[] tokens;
	private final ArrayDeque<Token> activeRing = new ArrayDeque<>();
	private final ArrayDeque<Token> blockedRing = new ArrayDeque<>();
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
	}

	public synchronized void configure(double rate, Duration period, Instant now) {
		Objects.requireNonNull(period, "period must not be null");
		Objects.requireNonNull(now, NOW_REQUIRED);
		if (rate <= 0.0d || period.isNegative() || period.isZero()) {
			disabled = true;
			return;
		}
		long nanos = Math.max(1L, (long) (period.toNanos() / rate));
		int configuredActiveTokens = Math.max(1, Math.min(tokens.length, (int) Math.ceil(rate)));
		spacing = Duration.ofNanos(nanos);
		boolean wasDisabled = disabled;
		disabled = false;
		if (!initialized || wasDisabled) {
			initialize(configuredActiveTokens, now);
		} else {
			reconfigureActiveTokens(configuredActiveTokens, period, now);
		}
	}

	public synchronized Acquisition acquire(Instant now) {
		Objects.requireNonNull(now, NOW_REQUIRED);
		if (disabled) {
			return Acquisition.deny();
		}
		while (true) {
			Token token = activeRing.getFirst();
			if (token.availableAt.isAfter(now)) {
				return Acquisition.waitFor(Duration.between(now, token.availableAt));
			}
			if (isExpired(token, now)) {
				recycle(token, now);
				rotateActiveHeadToTail();
				continue;
			}
			recycle(token, now);
			rotateActiveHeadToTail();
			return Acquisition.grant();
		}
	}

	public synchronized int availablePermits(Instant now, int max) {
		Objects.requireNonNull(now, NOW_REQUIRED);
		if (max <= 0 || disabled) {
			return 0;
		}
		skipExpired(now);
		int available = 0;
		Iterator<Token> iterator = activeRing.iterator();
		while (available < max && iterator.hasNext()) {
			Token token = iterator.next();
			if (token.availableAt.isAfter(now) || isExpired(token, now)) {
				return available;
			}
			available++;
		}
		return available;
	}

	public synchronized Duration timeUntilNextPermit(Instant now) {
		Objects.requireNonNull(now, NOW_REQUIRED);
		if (disabled) {
			return Duration.ZERO;
		}
		skipExpired(now);
		Token token = activeRing.getFirst();
		return token.availableAt.isAfter(now) ? Duration.between(now, token.availableAt) : Duration.ZERO;
	}

	Token headToken() {
		return activeRing.getFirst();
	}

	Token tokenAt(int index) {
		return tokens[index];
	}

	private void initialize(int configuredActiveTokens, Instant now) {
		activeRing.clear();
		blockedRing.clear();
		for (int i = 0; i < tokens.length; i++) {
			if (i < configuredActiveTokens) {
				tokens[i].schedule(now.plus(spacing.multipliedBy(i)), spacing);
				activeRing.addLast(tokens[i]);
			} else {
				tokens[i].disable();
				blockedRing.addLast(tokens[i]);
			}
		}
		initialized = true;
	}

	private void reconfigureActiveTokens(int configuredActiveTokens, Duration period, Instant now) {
		if (configuredActiveTokens == activeRing.size()) {
			return;
		}
		if (configuredActiveTokens < activeRing.size()) {
			while (activeRing.size() > configuredActiveTokens) {
				Token token = activeRing.removeLast();
				token.disable();
				blockedRing.addLast(token);
			}
			return;
		}
		Instant nextAvailable = latestScheduledToken().plus(spacing);
		Instant nextPeriod = now.plus(period);
		if (nextAvailable.isBefore(nextPeriod)) {
			nextAvailable = nextPeriod;
		}
		while (activeRing.size() < configuredActiveTokens) {
			Token token = blockedRing.removeFirst();
			token.schedule(nextAvailable, spacing);
			activeRing.addLast(token);
			nextAvailable = nextAvailable.plus(spacing);
		}
	}

	private void skipExpired(Instant now) {
		while (isExpired(activeRing.getFirst(), now)) {
			recycle(activeRing.getFirst(), now);
			rotateActiveHeadToTail();
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
		for (Token token : activeRing) {
			Instant availableAt = token.availableAt;
			if (availableAt.isAfter(latest)) {
				latest = availableAt;
			}
		}
		return latest;
	}

	private void rotateActiveHeadToTail() {
		activeRing.addLast(activeRing.removeFirst());
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
