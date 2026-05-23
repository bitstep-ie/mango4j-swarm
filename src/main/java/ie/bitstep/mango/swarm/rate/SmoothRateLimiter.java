package ie.bitstep.mango.swarm.rate;

import java.time.Duration;
import java.time.Instant;

/**
 * Time-slot based local rate limiter that spaces permits smoothly.
 */
public final class SmoothRateLimiter {
    private Instant nextSlot = Instant.EPOCH;
    private Duration spacing = Duration.ofSeconds(1);
    private boolean initialized;
    private boolean disabled;

    public synchronized void configure(double permits, Duration period, Instant now) {
        if (permits <= 0) {
            spacing = period;
            disabled = true;
        } else {
            long nanos = Math.max(1L, (long) (period.toNanos() / permits));
            spacing = Duration.ofNanos(nanos);
            disabled = false;
        }
        if (initialized && nextSlot.isBefore(now.minus(period))) {
            nextSlot = now.minus(period);
        }
    }

    public synchronized int permitsAvailable(Instant now, int max) {
        if (max <= 0 || disabled) {
            return 0;
        }
        Instant latestClaimableSlot = now.plus(spacing.multipliedBy(max));
        Instant oldestUsefulSlot = now.minus(spacing.multipliedBy(Math.max(0, max - 1)));
        if (!initialized) {
            initialized = true;
            nextSlot = oldestUsefulSlot;
        }
        if (nextSlot.isBefore(oldestUsefulSlot)) {
            nextSlot = oldestUsefulSlot;
        }
        if (latestClaimableSlot.isBefore(nextSlot)) {
            return 0;
        }
        int permits = 0;
        Instant cursor = nextSlot;
        while (permits < max && !cursor.isAfter(latestClaimableSlot)) {
            permits++;
            cursor = cursor.plus(spacing);
        }
        nextSlot = cursor;
        return permits;
    }

    public synchronized Duration timeUntilNextPermit(Instant now) {
        if (!initialized || !now.isBefore(nextSlot)) {
            return Duration.ZERO;
        }
        return Duration.between(now, nextSlot);
    }
}
