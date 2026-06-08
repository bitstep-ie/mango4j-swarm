# mango-swarm Schema

This document describes the PostgreSQL tables and indexes required by `mango-swarm`.

Applications own schema creation, table creation, and migration rollout. The library does not run migrations from its jar. The reference DDL is the sample app migration at [`../examples/reference-email-app/src/main/resources/db/migration/V1__mango_swarm.sql`](../examples/reference-email-app/src/main/resources/db/migration/V1__mango_swarm.sql). The test migration at [`../src/test/resources/db/migration/V1__mango_swarm.sql`](../src/test/resources/db/migration/V1__mango_swarm.sql) and the copyable SQL in [`mango-swarm-schema.sql`](mango-swarm-schema.sql) should match the sample app migration exactly.

## Tables

`mango-swarm` uses three tables:

- `mango_swarm_workers`: live worker membership and heartbeat state
- `mango_swarm_tasks`: durable task payloads and lifecycle state
- `mango_swarm_task_runtime`: hot mutable runtime/progress state for the current attempt

Task IDs are generated in Java as UUIDv7 values. UUIDv7 keeps task ids globally unique while preserving insertion locality better than random UUIDv4 values.

## `mango_swarm_workers`

Worker rows represent currently known daemon instances.

| Column | Type | Required | Purpose |
| --- | --- | --- | --- |
| `worker_id` | `uuid` | yes | Primary key for a daemon instance. Generated at daemon startup. |
| `hostname` | `text` | no | Diagnostic host name for the worker. |
| `started_at` | `timestamptz` | yes | Time the daemon instance started. |
| `last_heartbeat_at` | `timestamptz` | yes | Most recent heartbeat time. Used for active-worker counting and stale-worker cleanup. |

Each heartbeat updates the worker's own row, deletes stale rows, and counts non-stale rows. That count divides each configured task-type rate across active workers.

There is intentionally no `last_heartbeat_at` index in the reference DDL. Worker cardinality is normally small enough that scanning this table on heartbeat is cheaper than maintaining another index. Add `mango_swarm_workers(last_heartbeat_at)` only for unusually large worker fleets or if stale worker cleanup becomes a measured bottleneck.

## `mango_swarm_tasks`

Task rows are the durable queue and execution history.

| Column | Type | Required | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | yes | Primary key. Generated as UUIDv7 by the library. |
| `task_type` | `text` | yes | Configured task type key used to route work to a handler. |
| `payload` | `jsonb` | yes | Durable task payload. |
| `status` | `text` | yes | Current lifecycle state: `queued`, `claimed`, `in_progress`, `completed`, or `failed`. |
| `available_at` | `timestamptz` | yes | Earliest time a worker may claim the row. |
| `claimed_by` | `uuid` | no | Worker id that currently owns the claim. |
| `claimed_at` | `timestamptz` | no | Time the current attempt was claimed. |
| `attempt_count` | `integer` | yes | Number of times the row has been claimed. Starts at `0` and increments on claim. |
| `created_at` | `timestamptz` | yes | Row creation time. |
| `updated_at` | `timestamptz` | yes | Last durable lifecycle update time. |
| `completed_at` | `timestamptz` | no | Completion time for terminal successful rows. |
| `failed_at` | `timestamptz` | no | Failure time for terminal failed rows. |
| `execution_time_ms` | `bigint` | no | Final or durable lifecycle execution time in milliseconds. Must be non-negative when present. |
| `last_error_message` | `text` | no | Most recent failure, timeout, or requeue message. |

The `status` check constraint keeps lifecycle values within the states the repository understands. `execution_time_ms` is nullable because queued and newly claimed rows do not yet have an execution duration.

`available_at` is earliest database eligibility, not a start guarantee. A row also needs worker capacity, task-type concurrency, and local rate capacity before it can start.

## `mango_swarm_task_runtime`

Runtime rows hold frequently changing progress and liveness state for the current attempt.

| Column | Type | Required | Purpose |
| --- | --- | --- | --- |
| `task_id` | `uuid` | yes | Primary key and foreign key to `mango_swarm_tasks(id)`. |
| `worker_id` | `uuid` | yes | Worker id reporting runtime state for the current attempt. |
| `execution_state` | `text` | yes | Human-readable current state, for example `running`, `completed`, or `failed`. |
| `progress_percent` | `integer` | no | Optional progress from `0` to `100`. |
| `progress_message` | `text` | no | Optional human-readable progress message. |
| `started_at` | `timestamptz` | yes | Start time for the current attempt. |
| `updated_at` | `timestamptz` | yes | Last runtime update time. Used by timeout recovery as the liveness timestamp. |
| `execution_time_ms` | `bigint` | yes | Current elapsed attempt time in milliseconds. Must be non-negative. |

The primary key is also the foreign key and uses `ON DELETE CASCADE`, so deleting a task row automatically removes its runtime row.

This table is intentionally narrow and excludes the JSON payload. It uses `fillfactor = 75` in PostgreSQL to leave room on each page for frequent progress updates.

## Indexes

### `idx_mango_tasks_queue_claim`

```sql
CREATE INDEX IF NOT EXISTS idx_mango_tasks_queue_claim
    ON mango_swarm_tasks (task_type, available_at, id)
    WHERE status = 'queued';
```

Supports the hot claim path:

```sql
WHERE task_type = ?
AND status = 'queued'
AND available_at <= ?
ORDER BY available_at, id
LIMIT ?
FOR UPDATE SKIP LOCKED
```

This is the most important index for normal operation. It narrows by task type, keeps only queued rows in the index, and serves the deterministic claim order.

### `idx_mango_tasks_timeout_due`

```sql
CREATE INDEX IF NOT EXISTS idx_mango_tasks_timeout_due
    ON mango_swarm_tasks (task_type, claimed_at, id)
    WHERE status IN ('claimed', 'in_progress');
```

Supports timeout recovery for claimed or running tasks. The index narrows candidates by task type and partial status predicate.

Timeout recovery orders by `COALESCE(mango_swarm_task_runtime.updated_at, mango_swarm_tasks.claimed_at)`, so PostgreSQL cannot fully satisfy the ordering from this index after joining runtime rows. That is acceptable because timeout recovery is a batched maintenance path, not the hot claim path.

### `idx_mango_tasks_completed_cleanup`

```sql
CREATE INDEX IF NOT EXISTS idx_mango_tasks_completed_cleanup
    ON mango_swarm_tasks (completed_at, id)
    WHERE status = 'completed';
```

Supports retention cleanup of completed rows ordered by terminal timestamp.

### `idx_mango_tasks_failed_cleanup`

```sql
CREATE INDEX IF NOT EXISTS idx_mango_tasks_failed_cleanup
    ON mango_swarm_tasks (failed_at, id)
    WHERE status = 'failed';
```

Supports retention cleanup of failed rows ordered by terminal timestamp.

## Lifecycle

Common state transitions:

| Transition | Table effects |
| --- | --- |
| Queue | Insert `mango_swarm_tasks` row with `status = 'queued'` and the requested `available_at`. |
| Claim | Update task to `claimed`, set `claimed_by`, set `claimed_at`, increment `attempt_count`. |
| Start | Update task to `in_progress`; insert/reset the runtime row with `execution_state = 'running'`. |
| Progress | Update only `mango_swarm_task_runtime`. |
| Complete | Update task to `completed`, set `completed_at`, set final `execution_time_ms`, and write final runtime state. |
| Retry | Update task back to `queued`, set a new `available_at`, clear claim fields, and delete runtime state. |
| Fail | Update task to `failed`, set `failed_at`, set `last_error_message`, and write failure runtime state. |
| Timeout reclaim | Requeue or fail stale claimed/in-progress rows depending on task-type idempotency and reclaim configuration. |
| Cleanup | Delete old completed/failed task rows in bounded batches. Runtime rows are removed by cascade. |

## Operational Notes

- Keep `mango_swarm_task_runtime` free of large payloads or application data; it is the hot update table.
- Do not add a runtime index unless there is a real query path for it. Runtime progress updates pay the write cost for every maintained index.
- Use the provided partial indexes for the task table. They keep queue, timeout, and cleanup indexes smaller than full-table indexes.
- Existing deployments that already created an unused runtime worker index can drop it with:

```sql
DROP INDEX IF EXISTS idx_mango_task_runtime_worker;
```

- If worker counts become very large, consider adding:

```sql
CREATE INDEX IF NOT EXISTS idx_mango_workers_last_heartbeat
    ON mango_swarm_workers (last_heartbeat_at);
```

Measure before adding it for normal deployments.
