CREATE TABLE IF NOT EXISTS mango_swarm_workers (
    worker_id uuid PRIMARY KEY,
    hostname text,
    started_at timestamptz NOT NULL,
    last_heartbeat_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mango_workers_last_heartbeat
    ON mango_swarm_workers (last_heartbeat_at);

CREATE TABLE IF NOT EXISTS mango_swarm_task_pacers (
    task_type text NOT NULL,
    slot_at timestamptz NOT NULL,
    task_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_type, slot_at)
);

CREATE INDEX IF NOT EXISTS idx_mango_task_pacers_task_type_slot
    ON mango_swarm_task_pacers (task_type, slot_at);

CREATE TABLE IF NOT EXISTS mango_swarm_tasks (
    id uuid PRIMARY KEY,
    task_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('queued', 'claimed', 'in_progress', 'completed', 'failed')),
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_by uuid NULL,
    claimed_at timestamptz NULL,
    progress_percent integer NULL CHECK (progress_percent BETWEEN 0 AND 100),
    progress_description text NULL,
    last_progress_at timestamptz NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz NULL,
    failed_at timestamptz NULL,
    last_error_message text NULL
);

CREATE INDEX IF NOT EXISTS idx_mango_tasks_queue_claim
    ON mango_swarm_tasks (task_type, available_at, id)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_mango_tasks_type_available
    ON mango_swarm_tasks (task_type, status, available_at);

CREATE INDEX IF NOT EXISTS idx_mango_tasks_stale_claimed
    ON mango_swarm_tasks (task_type, claimed_at, last_progress_at)
    WHERE status IN ('claimed', 'in_progress');

CREATE INDEX IF NOT EXISTS idx_mango_tasks_claimed_by
    ON mango_swarm_tasks (claimed_by, status);
