package ie.bitstep.mango.examples.email;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "reference.email")
public record ReferenceEmailSeedProperties(
        int seedCount,
        Duration seedInterval,
        Duration seedInitialDelay,
        boolean scheduleFutureTask) {

    public ReferenceEmailSeedProperties {
        if (seedCount <= 0) {
            seedCount = 20;
        }
        if (seedInterval == null || seedInterval.isNegative() || seedInterval.isZero()) {
            seedInterval = Duration.ofSeconds(15);
        }
        if (seedInitialDelay == null || seedInitialDelay.isNegative()) {
            seedInitialDelay = Duration.ofSeconds(3);
        }
    }
}
