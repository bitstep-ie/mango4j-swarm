package ie.bitstep.mango.swarm;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UuidV7Test {

	@Test
	void generatedUuidHasVersionSevenVariantAndTimestampBits() {
		Instant now = Instant.parse("2026-05-25T10:15:30.123Z");

		UUID id = UuidV7.generate(now);

		assertThat(id.version()).isEqualTo(7);
		assertThat(id.variant()).isEqualTo(2);
		assertThat((id.getMostSignificantBits() >>> 16) & 0x0000_ffff_ffff_ffffL)
				.isEqualTo(now.toEpochMilli());
		assertThat(id.getLeastSignificantBits() & 0xc000_0000_0000_0000L).isEqualTo(0x8000_0000_0000_0000L);
	}

	@Test
	void generatedIdsAreUnique() {
		Instant now = Instant.parse("2026-05-25T10:15:30.123Z");

		List<UUID> ids =
				IntStream.range(0, 100).mapToObj(i -> UuidV7.generate(now)).toList();

		assertThat(ids).doesNotHaveDuplicates().allMatch(id -> id.version() == 7);
	}

	@Test
	void randomFieldIsMaskedRatherThanForcedToAllOnes() {
		Instant now = Instant.parse("2026-05-25T10:15:30.123Z");

		assertThat(java.util.stream.IntStream.range(0, 16)
						.mapToObj(ignored -> UuidV7.generate(now))
						.mapToLong(id -> id.getMostSignificantBits() & 0x0fffl)
						.allMatch(randomA -> randomA == 0x0fffl))
				.isFalse();
		assertThat(java.util.stream.IntStream.range(0, 16)
						.mapToObj(ignored -> UuidV7.generate(now))
						.mapToLong(id -> id.getLeastSignificantBits() & 0x3fff_ffff_ffff_ffffL)
						.allMatch(randomB -> randomB == 0x3fff_ffff_ffff_ffffL))
				.isFalse();
	}
}
