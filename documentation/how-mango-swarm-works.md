# How mango-swarm Works

This document explains the runtime model of `mango-swarm`, the table interactions, and the control flow from task submission to completion.

## Scope and assumptions

- Multiple instances of the same application can run at the same time.
- All instances support the same configured task types.
- Task payloads are durable JSON (`jsonb`) and can outlive Java POJO versions.
- The application owns schema and table creation. The library does not run migrations from its jar.

## Core components

- `MangoTasks`: high-level API for `queue(...)`, `at(...)`, and `after(...)`.
- `MangoSwarmDaemon`: worker loop that heartbeats, claims, dispatches, retries, reclaims, and cleans up old terminal tasks.
- `TaskHandler<T>`: application task logic.
- `TaskExecutionContext<T>`: metadata + payload + progress reporting callback.
- `TaskRepository`: PostgreSQL persistence contract used by the daemon.
- `WorkerRegistry`: worker heartbeat and active-worker counting.

## Data model

Required tables:

- `mango_swarm_workers`
  - worker identity and heartbeats (`worker_id`, `last_heartbeat_at`, etc.)
- `mango_swarm_tasks`
  - durable work item and lifecycle fields
  - includes progress liveness fields:
    - `progress_percent`
    - `progress_description`
    - `last_progress_at`
- `mango_swarm_task_pacers`
  - per-task-type slot occupancy ledger used for smooth scheduling

Reference SQL:

- `documentation/mango-swarm-schema.sql`

## End-to-end runtime flow

```mermaid
sequenceDiagram
    autonumber
    participant App as App Code
    participant API as MangoTasks
    participant DB as PostgreSQL
    participant D as MangoSwarmDaemon
    participant H as TaskHandler

    App->>API: queue/at/after(taskType, payload)
    API->>DB: reserve slot in mango_swarm_task_pacers
    API->>DB: insert row in mango_swarm_tasks(status=queued)

    loop Poll cycle
        D->>DB: heartbeat worker + prune stale workers
        D->>DB: cleanup old completed/failed rows (interval-based)
        D->>D: recalc effective rate = app rate / active workers
        D->>DB: select claimable batch
        D->>DB: mark claimed rows
        D->>DB: mark task in_progress
        D->>H: execute(TaskExecutionContext<T>)
        H->>D: context.progress(percent, description)*
        D->>DB: record progress_percent/description/last_progress_at
        alt success
            D->>DB: mark completed + set 100/finished
        else failed result/exception and attempts remain
            D->>DB: reschedule same row to queued at retry time
        else failed with no attempts left
            D->>DB: mark failed
        end
    end
```

`*` progress calls are optional but strongly recommended for long-running handlers.

## Task state lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued
    queued --> claimed: claim batch
    claimed --> in_progress: dispatch start
    in_progress --> completed: handler success
    in_progress --> queued: retry/reschedule
    claimed --> queued: timeout reclaim (idempotent + reclaim enabled)
    in_progress --> queued: timeout reclaim (idempotent + reclaim enabled)
    claimed --> failed: timeout + reclaim disabled
    in_progress --> failed: timeout + reclaim disabled
    in_progress --> failed: retries exhausted
    completed --> [*]
    failed --> [*]
```

## Scheduling model

`MangoTasks` schedules by slot spacing derived from task-type `rate` and `period`:

- `slotSpacing = period / rate` (bounded to at least 1ns)

When queueing:

1. Check nearest occupied slot at or before requested time.
2. Check nearest occupied slot after requested time.
3. Move requested time forward only when needed to avoid slot collisions.

Result: a far-future task does not block earlier tasks.

## Distributed rate division

Each task type has app-level rate config. A worker applies:

- `effectiveLocalRate = configuredRate / activeWorkerCount`

`activeWorkerCount` comes from worker heartbeats in `mango_swarm_workers`.

The daemon uses smooth slot pacing and batch claiming, not burst-all-at-once permits.

## Batch claiming and concurrency

For each task type and poll cycle, claim limit is bounded by:

- configured/derived batch size
- remaining local rate capacity
- remaining per-task-type concurrency
- remaining global executor capacity

Claiming uses a portable select-and-update flow so repository behavior can be validated against H2. PostgreSQL-specific concurrent row-lock claiming is outside the H2 test surface.

## Progress and liveness

Handlers receive `TaskExecutionContext<T>` and can call:

- `progress(percent)`
- `progress(percent, description)`

`TaskExecutionContext<T>` contains:

- `taskId`: persisted task UUID from `mango_swarm_tasks.id`
- `taskType`: configured task type key
- `workerId`: worker UUID executing the attempt
- `attemptCount`: current attempt number
- `claimedAt`: claim timestamp for the current attempt
- `payload`: extracted typed payload

Effects of each call:

- updates `progress_percent`
- updates `progress_description` (if provided)
- updates `last_progress_at`

Timeout reclaim checks:

- `COALESCE(last_progress_at, claimed_at)`

So progress calls extend the reclaim silence window.

On successful completion, the library records:

- `progress_percent = 100`
- `progress_description = finished`

## Retries and reclaim

Failure retry:

- same row is rescheduled (`status=queued`, `available_at=retryAt`)
- delay uses exponential backoff (global defaults + per-task overrides)

Timeout reclaim:

- only requeues when:
  - `reclaim-on-timeout = true`
  - `idempotent = true`
- otherwise timeout path marks tasks as failed

## Cleanup task

The daemon also runs a built-in retention cleanup pass. No application `TaskHandler` is required.

Cleanup config:

- `mango.swarm.cleanup.enabled` (default `true`)
- `mango.swarm.cleanup.interval` (default `10m`)
- `mango.swarm.cleanup.completed-retention` (default `30d`)
- `mango.swarm.cleanup.failed-retention` (default `90d`)

Cleanup behavior:

- delete from `mango_swarm_tasks` where `status='completed'` and `completed_at < now - completed-retention`
- delete from `mango_swarm_tasks` where `status='failed'` and `failed_at < now - failed-retention`
- never deletes `queued`, `claimed`, or `in_progress` rows

This keeps the task table bounded while preserving recent execution history for diagnostics.

## Threading model

- Global executor capacity is independent of per-task-type concurrency.
- Per-task-type concurrency caps a single type.
- Global pool caps total parallel work on the worker.
- Virtual threads can be enabled/auto-detected at runtime (Java 21+), while baseline compile target remains Java 17.

## What application teams own

- Schema creation and selection.
- Table creation and migration lifecycle.
- Task handler implementations.
- Task-type config (`rate`, `period`, `concurrency`, `timeout`, retries, reclaim/idempotency).
