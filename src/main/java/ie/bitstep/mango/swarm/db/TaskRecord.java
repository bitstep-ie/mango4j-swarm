package ie.bitstep.mango.swarm.db;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import ie.bitstep.mango.swarm.TaskStatus;

/** Immutable task row projection used by repository claim/load operations. */
public record TaskRecord(
		UUID id,
		String taskType,
		JsonNode payload,
		TaskStatus status,
		Instant availableAt,
		UUID claimedBy,
		Instant claimedAt,
		int attemptCount,
		Instant createdAt,
		Instant updatedAt) {}
