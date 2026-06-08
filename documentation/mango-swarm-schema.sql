CREATE TABLE IF NOT EXISTS mango_swarm_workers (
    worker_id uuid PRIMARY KEY,
    hostname text,
    started_at timestamptz NOT NULL,
    last_heartbeat_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS mango_swarm_tasks (
    id uuid PRIMARY KEY,
    task_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('queued', 'claimed', 'in_progress', 'completed', 'failed')),
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_by uuid NULL,
    claimed_at timestamptz NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz NULL,
    failed_at timestamptz NULL,
    execution_time_ms bigint NULL CHECK (execution_time_ms >= 0),
    last_error_message text NULL
);

CREATE TABLE IF NOT EXISTS mango_swarm_task_runtime (
    task_id uuid PRIMARY KEY REFERENCES mango_swarm_tasks(id) ON DELETE CASCADE,
    worker_id uuid NOT NULL,
    execution_state text NOT NULL,
    progress_percent integer NULL CHECK (progress_percent BETWEEN 0 AND 100),
    progress_message text NULL,
    started_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    execution_time_ms bigint NOT NULL DEFAULT 0 CHECK (execution_time_ms >= 0)
) WITH (fillfactor = 75);

CREATE INDEX IF NOT EXISTS idx_mango_tasks_queue_claim
    ON mango_swarm_tasks (task_type, available_at, id)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_mango_tasks_timeout_due
    ON mango_swarm_tasks (task_type, claimed_at, id)
    WHERE status IN ('claimed', 'in_progress');

CREATE INDEX IF NOT EXISTS idx_mango_tasks_completed_cleanup
    ON mango_swarm_tasks (completed_at, id)
    WHERE status = 'completed';

CREATE INDEX IF NOT EXISTS idx_mango_tasks_failed_cleanup
    ON mango_swarm_tasks (failed_at, id)
    WHERE status = 'failed';
