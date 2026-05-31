package ie.bitstep.mango.swarm;

/** Persistent task lifecycle states stored in {@code mango_swarm_tasks.status}. */
public enum TaskStatus {
	/** Waiting for claim. */
	QUEUED("queued"),
	/** Claimed by a worker, awaiting task execution start. */
	CLAIMED("claimed"),
	/** Currently running in a worker executor thread. */
	IN_PROGRESS("in_progress"),
	/** Finished successfully. */
	COMPLETED("completed"),
	/** Finished unsuccessfully with no remaining retries. */
	FAILED("failed");

	private final String databaseValue;

	TaskStatus(String databaseValue) {
		this.databaseValue = databaseValue;
	}

	public String databaseValue() {
		return databaseValue;
	}

	public static TaskStatus fromDatabaseValue(String value) {
		for (TaskStatus status : values()) {
			if (status.databaseValue.equals(value)) {
				return status;
			}
		}
		throw new IllegalArgumentException("Unknown task status: " + value);
	}
}
