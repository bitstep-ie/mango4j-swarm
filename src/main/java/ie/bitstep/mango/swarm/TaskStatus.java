package ie.bitstep.mango.swarm;

/** Persistent task lifecycle states stored in {@code mango_swarm_tasks.status}. */
public enum TaskStatus {
    /** Waiting for claim. */
    queued,
    /** Claimed by a worker, awaiting task execution start. */
    claimed,
    /** Currently running in a worker executor thread. */
    in_progress,
    /** Finished successfully. */
    completed,
    /** Finished unsuccessfully with no remaining retries. */
    failed
}
