package ie.bitstep.mango.swarm.executor;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskConcurrencyTrackerTest {

    @Test
    void tracksConfiguredAndDefaultConcurrencyLimits() {
        TaskConcurrencyTracker tracker = new TaskConcurrencyTracker(Map.of("email", 2));

        assertThat(tracker.remaining("email")).isEqualTo(2);
        assertThat(tracker.tryAcquire("email")).isTrue();
        assertThat(tracker.tryAcquire("email")).isTrue();
        assertThat(tracker.remaining("email")).isZero();
        assertThat(tracker.tryAcquire("email")).isFalse();

        tracker.release("email");
        assertThat(tracker.remaining("email")).isEqualTo(1);

        assertThat(tracker.tryAcquire("unknown")).isTrue();
        assertThat(tracker.tryAcquire("unknown")).isFalse();
    }
}
