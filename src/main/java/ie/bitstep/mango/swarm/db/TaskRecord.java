package ie.bitstep.mango.swarm.db;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import ie.bitstep.mango.swarm.TaskStatus;

/** Immutable task row projection used by repository claim/load operations. */
public final class TaskRecord {
	private final UUID id;
	private final String taskType;
	private final JsonNode payload;
	private final TaskStatus status;
	private final Instant availableAt;
	private final UUID claimedBy;
	private final Instant claimedAt;
	private final int attemptCount;
	private final Instant createdAt;
	private final Instant updatedAt;

	public TaskRecord(
			UUID id,
			String taskType,
			JsonNode payload,
			TaskStatus status,
			Instant availableAt,
			UUID claimedBy,
			Instant claimedAt,
			int attemptCount,
			Instant createdAt,
			Instant updatedAt) {
		this.id = id;
		this.taskType = taskType;
		this.payload = payload;
		this.status = status;
		this.availableAt = availableAt;
		this.claimedBy = claimedBy;
		this.claimedAt = claimedAt;
		this.attemptCount = attemptCount;
		this.createdAt = createdAt;
		this.updatedAt = updatedAt;
	}

	public UUID id() {
		return id;
	}

	public String taskType() {
		return taskType;
	}

	public JsonNode payload() {
		return payload;
	}

	public TaskStatus status() {
		return status;
	}

	public Instant availableAt() {
		return availableAt;
	}

	public UUID claimedBy() {
		return claimedBy;
	}

	public Instant claimedAt() {
		return claimedAt;
	}

	public int attemptCount() {
		return attemptCount;
	}

	public Instant createdAt() {
		return createdAt;
	}

	public Instant updatedAt() {
		return updatedAt;
	}
}
