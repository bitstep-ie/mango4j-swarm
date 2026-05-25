package ie.bitstep.mango.swarm.rate;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SmoothRateLimiterTest {

    @Test
    void spacesPermitsAcrossPeriodAndReportsWaitUntilNextSlot() {
        SmoothRateLimiter limiter = new SmoothRateLimiter();
        Instant now = Instant.parse("2026-05-25T10:00:00Z");
        limiter.configure(2, Duration.ofSeconds(1), now);

        int initial = limiter.permitsAvailable(now, 2);
        Duration wait = limiter.timeUntilNextPermit(now);
        int noneBeforeNextSlot = limiter.permitsAvailable(now.minusMillis(1), 1);
        int next = limiter.permitsAvailable(now.plusMillis(500), 1);

        assertThat(initial).isEqualTo(2);
        assertThat(wait).isEqualTo(Duration.ofMillis(500));
        assertThat(noneBeforeNextSlot).isZero();
        assertThat(next).isEqualTo(1);
    }

    @Test
    void disablesPermitsWhenRateIsNotPositive() {
        SmoothRateLimiter limiter = new SmoothRateLimiter();
        Instant now = Instant.parse("2026-05-25T10:00:00Z");

        limiter.configure(0, Duration.ofSeconds(1), now);

        assertThat(limiter.permitsAvailable(now, 10)).isZero();
        assertThat(limiter.permitsAvailable(now, 0)).isZero();
        assertThat(limiter.timeUntilNextPermit(now)).isEqualTo(Duration.ZERO);

        limiter.configure(1, Duration.ofSeconds(1), now.plusSeconds(1));
        assertThat(limiter.permitsAvailable(now.plusSeconds(1), 1)).isEqualTo(1);
    }

    @Test
    void zeroMaxDoesNotConsumeNextAvailablePermit() {
        SmoothRateLimiter limiter = new SmoothRateLimiter();
        Instant now = Instant.parse("2026-05-25T10:00:00Z");
        limiter.configure(1, Duration.ofSeconds(1), now);

        assertThat(limiter.permitsAvailable(now, 0)).isZero();
        assertThat(limiter.permitsAvailable(now, 1)).isEqualTo(1);
    }

    @Test
    void reportsNoPermitsWhenRequestIsBeforeNextClaimableSlot() {
        SmoothRateLimiter limiter = new SmoothRateLimiter();
        Instant now = Instant.parse("2026-05-25T10:00:00Z");
        limiter.configure(1, Duration.ofSeconds(1), now);

        assertThat(limiter.permitsAvailable(now, 1)).isEqualTo(1);
        assertThat(limiter.permitsAvailable(now.minusMillis(2), 1)).isZero();
    }

    @Test
    void negativeRatesDisablePermitsAndExactLatestSlotIsClaimable() {
        SmoothRateLimiter disabled = new SmoothRateLimiter();
        Instant now = Instant.parse("2026-05-25T10:00:00Z");
        disabled.configure(-1, Duration.ofSeconds(1), now);

        assertThat(disabled.permitsAvailable(now, 1)).isZero();

        SmoothRateLimiter limiter = new SmoothRateLimiter();
        limiter.configure(1, Duration.ofSeconds(1), now);
        assertThat(limiter.permitsAvailable(now, 1)).isEqualTo(1);
        assertThat(limiter.permitsAvailable(now, 1)).isEqualTo(1);
    }

    @Test
    void doesNotLetStaleNextSlotGrowWithoutBoundAfterReconfigure() {
        SmoothRateLimiter limiter = new SmoothRateLimiter();
        Instant first = Instant.parse("2026-05-25T10:00:00Z");
        Instant later = first.plusSeconds(10);
        limiter.configure(1, Duration.ofSeconds(1), first);
        limiter.permitsAvailable(first, 1);

        limiter.configure(1, Duration.ofSeconds(1), later);
        int permits = limiter.permitsAvailable(later, 1);

        assertThat(permits).isEqualTo(1);
    }
}
