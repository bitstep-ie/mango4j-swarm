package ie.bitstep.mango.swarm.rate;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalTokenRingRateLimiterTest {
	private static final Instant NOW = Instant.parse("2026-06-04T10:00:00Z");

	@Test
	void capacityMustBePositive() {
		assertThatThrownBy(() -> new LocalTokenRingRateLimiter(0))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("capacity must be at least 1");
	}

	@Test
	void invalidConfigurationDisablesLimiterUntilReconfigured() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);

		limiter.configure(0, Duration.ofSeconds(2), NOW);

		assertThat(limiter.acquire(NOW).granted()).isFalse();
		assertThat(limiter.availablePermits(NOW, 2)).isZero();
		assertThat(limiter.timeUntilNextPermit(NOW)).isEqualTo(Duration.ZERO);

		Instant later = NOW.plusSeconds(10);
		limiter.configure(2, Duration.ofSeconds(2), later);

		assertThat(limiter.acquire(later).granted()).isTrue();
		assertThat(limiter.headToken().availableAt()).isEqualTo(later.plusSeconds(1));
	}

	@Test
	void negativePeriodDisablesLimiter() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);

		limiter.configure(2, Duration.ofNanos(-1), NOW);

		assertThat(limiter.acquire(NOW).granted()).isFalse();
		assertThat(limiter.availablePermits(NOW, 2)).isZero();
	}

	@Test
	void zeroPeriodDisablesLimiter() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);

		limiter.configure(2, Duration.ZERO, NOW);

		assertThat(limiter.acquire(NOW).granted()).isFalse();
		assertThat(limiter.availablePermits(NOW, 2)).isZero();
	}

	@Test
	void zeroOrNegativeMaxHasNoAvailablePermits() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);

		assertThat(limiter.availablePermits(NOW, 0)).isZero();
		assertThat(limiter.availablePermits(NOW, -1)).isZero();
	}

	@Test
	void disabledLimiterHasNoAvailablePermitsForPositiveMax() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);

		assertThat(limiter.availablePermits(NOW, 1)).isZero();
	}

	@Test
	void reconfiguringSameActiveTokenCountPreservesSchedule() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);
		limiter.acquire(NOW);

		limiter.configure(2, Duration.ofSeconds(2), NOW.plusMillis(100));

		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(1));
		assertThat(limiter.tokenAt(0).availableAt()).isEqualTo(NOW.plusSeconds(2));
		assertThat(limiter.tokenAt(1).availableAt()).isEqualTo(NOW.plusSeconds(1));
	}

	@Test
	void tokenIsNotAvailableBeforeAvailableAt() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(1);
		limiter.configure(1, Duration.ofSeconds(1), NOW);

		limiter.acquire(NOW);
		LocalTokenRingRateLimiter.Acquisition acquisition = limiter.acquire(NOW.plusMillis(500));

		assertThat(acquisition.granted()).isFalse();
		assertThat(acquisition.waitDuration()).isEqualTo(Duration.ofMillis(500));
	}

	@Test
	void availablePermitsStopAtFutureToken() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);

		assertThat(limiter.availablePermits(NOW, 2)).isOne();
	}

	@Test
	void tokenIsValidWhenNowIsWithinTokenWindow() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(1);
		limiter.configure(1, Duration.ofSeconds(1), NOW);

		LocalTokenRingRateLimiter.Acquisition acquisition = limiter.acquire(NOW.plusMillis(999));

		assertThat(acquisition.granted()).isTrue();
	}

	@Test
	void tokenIsExpiredWhenNowIsAtExpiresAt() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(1);
		limiter.configure(1, Duration.ofSeconds(1), NOW);

		LocalTokenRingRateLimiter.Acquisition acquisition = limiter.acquire(NOW.plusSeconds(1));

		assertThat(acquisition.granted()).isFalse();
		assertThat(acquisition.waitDuration()).isEqualTo(Duration.ofSeconds(1));
		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(2));
	}

	@Test
	void expiredTokensAreRecycledWithoutReplacingTokenObject() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(1);
		limiter.configure(1, Duration.ofSeconds(1), NOW);
		LocalTokenRingRateLimiter.Token original = limiter.headToken();

		limiter.acquire(NOW.plusSeconds(5));

		assertThat(limiter.headToken()).isSameAs(original);
		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(6));
	}

	@Test
	void multipleExpiredHeadTokensAreSkippedUntilFutureTokenIsReached() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(3);
		limiter.configure(3, Duration.ofSeconds(3), NOW);

		LocalTokenRingRateLimiter.Acquisition acquisition = limiter.acquire(NOW.plusSeconds(10));

		assertThat(acquisition.granted()).isFalse();
		assertThat(acquisition.waitDuration()).isEqualTo(Duration.ofSeconds(1));
		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(11));
	}

	@Test
	void consumedTokensAreRecycledToNextFutureSlot() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(2), NOW);
		LocalTokenRingRateLimiter.Token consumed = limiter.headToken();

		LocalTokenRingRateLimiter.Acquisition acquisition = limiter.acquire(NOW);

		assertThat(acquisition.granted()).isTrue();
		assertThat(consumed.availableAt()).isEqualTo(NOW.plusSeconds(2));
		assertThat(consumed.expiresAt()).isEqualTo(NOW.plusSeconds(3));
		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(1));
	}

	@Test
	void reducingActiveTokenCountDoesNotRefreshRemainingTokens() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(4);
		limiter.configure(4, Duration.ofSeconds(4), NOW);
		limiter.acquire(NOW);

		limiter.configure(2, Duration.ofSeconds(4), NOW);

		assertThat(limiter.availablePermits(NOW, 2)).isZero();
		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(1));
		assertThat(limiter.tokenAt(0).availableAt()).isEqualTo(Instant.MAX);
		assertThat(limiter.tokenAt(1).availableAt()).isEqualTo(NOW.plusSeconds(1));
		assertThat(limiter.tokenAt(2).availableAt()).isEqualTo(NOW.plusSeconds(2));
		assertThat(limiter.tokenAt(3).availableAt()).isEqualTo(Instant.MAX);
	}

	@Test
	void increasingActiveTokenCountOnlySchedulesNewTokens() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(4);
		limiter.configure(2, Duration.ofSeconds(4), NOW);
		limiter.acquire(NOW);

		limiter.configure(4, Duration.ofSeconds(4), NOW);

		assertThat(limiter.availablePermits(NOW, 4)).isZero();
		assertThat(limiter.headToken().availableAt()).isEqualTo(NOW.plusSeconds(2));
		assertThat(limiter.tokenAt(0).availableAt()).isEqualTo(NOW.plusSeconds(4));
		assertThat(limiter.tokenAt(1).availableAt()).isEqualTo(NOW.plusSeconds(2));
		assertThat(limiter.tokenAt(2).availableAt()).isEqualTo(NOW.plusSeconds(5));
		assertThat(limiter.tokenAt(3).availableAt()).isEqualTo(NOW.plusSeconds(6));
	}

	@Test
	void increasingAfterReductionDefersNewTokensUntilNextPeriod() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(4);
		limiter.configure(4, Duration.ofSeconds(4), NOW);

		limiter.configure(2, Duration.ofSeconds(4), NOW);
		limiter.configure(4, Duration.ofSeconds(4), NOW);

		assertThat(limiter.availablePermits(NOW, 4)).isEqualTo(1);
		assertThat(limiter.tokenAt(0).availableAt()).isEqualTo(NOW);
		assertThat(limiter.tokenAt(1).availableAt()).isEqualTo(NOW.plusSeconds(1));
		assertThat(limiter.tokenAt(2).availableAt()).isEqualTo(NOW.plusSeconds(5));
		assertThat(limiter.tokenAt(3).availableAt()).isEqualTo(NOW.plusSeconds(4));
	}

	@Test
	void noBurstCatchUpOccursAfterLongPause() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(5);
		limiter.configure(5, Duration.ofSeconds(5), NOW);
		Instant later = NOW.plus(Duration.ofMinutes(1));

		assertThat(limiter.availablePermits(later, 5)).isZero();
		assertThat(limiter.acquire(later).granted()).isFalse();
	}

	@Test
	void effectiveStartRateNeverExceedsConfiguredRate() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(2);
		limiter.configure(2, Duration.ofSeconds(1), NOW);
		int starts = 0;

		for (int i = 0; i < 1_000; i++) {
			if (limiter.acquire(NOW.plusNanos(i * 1_000_000L)).granted()) {
				starts++;
			}
		}

		assertThat(starts).isEqualTo(2);
	}

	@Test
	void timeUntilNextPermitIsZeroWhenPermitIsAvailableAndPositiveWhenBlocked() {
		LocalTokenRingRateLimiter limiter = new LocalTokenRingRateLimiter(1);
		limiter.configure(1, Duration.ofSeconds(1), NOW);

		assertThat(limiter.timeUntilNextPermit(NOW)).isEqualTo(Duration.ZERO);

		limiter.acquire(NOW);

		assertThat(limiter.timeUntilNextPermit(NOW)).isEqualTo(Duration.ofSeconds(1));
		assertThat(limiter.timeUntilNextPermit(NOW.plusMillis(600))).isEqualTo(Duration.ofMillis(400));
	}

	@Test
	void separateTaskTypesHaveIndependentRings() {
		LocalTokenRingRateLimiter email = new LocalTokenRingRateLimiter(1);
		LocalTokenRingRateLimiter invoice = new LocalTokenRingRateLimiter(1);
		email.configure(1, Duration.ofSeconds(1), NOW);
		invoice.configure(1, Duration.ofSeconds(1), NOW);

		assertThat(email.acquire(NOW).granted()).isTrue();
		assertThat(invoice.acquire(NOW).granted()).isTrue();
	}

	@Test
	void separateWorkersHaveIndependentRings() {
		LocalTokenRingRateLimiter firstWorker = new LocalTokenRingRateLimiter(1);
		LocalTokenRingRateLimiter secondWorker = new LocalTokenRingRateLimiter(1);
		firstWorker.configure(1, Duration.ofSeconds(1), NOW);
		secondWorker.configure(1, Duration.ofSeconds(1), NOW);

		assertThat(firstWorker.acquire(NOW).granted()).isTrue();
		assertThat(secondWorker.acquire(NOW).granted()).isTrue();
	}
}
