package ie.bitstep.mango.swarm;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

/** UUIDv7 generator used for task identifiers. */
public final class UuidV7 {
	private static final SecureRandom RANDOM = new SecureRandom();

	private UuidV7() {
		throw new AssertionError("No instances");
	}

	public static UUID generate() {
		return generate(Instant.now());
	}

	static UUID generate(Instant now) {
		long epochMillis = now.toEpochMilli();
		long randomA = RANDOM.nextLong() & 0x0fffl;
		long randomB = RANDOM.nextLong() & 0x3fff_ffff_ffff_ffffL;

		long mostSignificantBits = ((epochMillis & 0x0000_ffff_ffff_ffffL) << 16) | 0x0000_0000_0000_7000L | randomA;
		long leastSignificantBits = 0x8000_0000_0000_0000L | randomB;
		return new UUID(mostSignificantBits, leastSignificantBits);
	}
}
