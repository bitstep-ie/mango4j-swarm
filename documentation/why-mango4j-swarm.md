# Why mango-swarm?

`mango-swarm` exists for a specific application problem: durable, rate-limited background task execution inside a Spring Boot service.

The current implementation is PostgreSQL-only. The persistence layer is still an implementation detail from the application model's point of view, but applications should plan for PostgreSQL when adopting mango-swarm today.

It is not trying to be a general job-processing platform, a workflow engine, or a replacement for every scheduler. It is built for application-owned work such as:

- sending email or notifications
- calling third-party APIs under strict rate limits
- processing delayed or retryable application tasks
- running many independent units of work across multiple service instances
- keeping queued work durable without adding a broker or workflow platform
- reporting progress for long-running tasks

## The Problem

Many Spring Boot applications eventually need more than `@Scheduled`.

A typical application needs to:

- queue work now, later, or after a delay
- survive process restarts
- avoid duplicate execution across multiple app instances
- retry failed work
- reclaim abandoned work after worker crashes
- pause, reject, or drop specific task types operationally
- enforce per-task-type concurrency
- enforce an application-wide rate limit across all workers
- avoid bursting a large backlog after downtime
- observe progress without constantly updating large durable task rows

These requirements sit between two common tools:

- ShedLock is good at making one scheduled method run on one instance.
- Spring Batch is good at structured batch jobs with readers, processors, writers, steps, and job metadata.

`mango-swarm` targets the gap between them: distributed durable task execution for many small independent tasks.

It is not merely a scheduler. It is a task-type execution control layer. Tasks may be queued now, scheduled with `at(...)` or `after(...)`, held from execution by task-type mode, activated later, retried according to configurable backoff policy, rejected or dropped according to task policy, and executed under distributed rate and concurrency limits.

## Comparison With Related Spring And Java Tools

Several Spring and Java tools overlap with parts of mango-swarm: background jobs, durable scheduling, retries, distributed execution, message persistence, or scheduled-method coordination. The differences are mostly about the shape of work each tool makes natural. For mango-swarm today, the durable store is PostgreSQL.

| Project | Fit vs Swarm | Immediate jobs | Future / delayed jobs | Recurring jobs | Durable execution | Multi-instance coordination | Run modes / queue-only | Reject / drop policy | Retry + backoff flexibility | Rate / pacing | Per-task concurrency | Timeout reclaim | Best fit |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mango4J Swarm | 10/10 | Yes, `queue(...)` | Yes, `at(...)` and `after(...)` | Possible by scheduling or requeueing, if applicable | Yes | Yes | Yes, task-type modes: `execute`, `queue`, `reject`, `drop` | Yes | Yes, global defaults and per-task-type overrides | Yes, distributed rate division plus local token-ring pacing | Yes, per task type | Yes, for idempotent task types with `reclaim-on-timeout` | Embedded durable task scheduling and controlled execution |
| JobRunr | 7.5/10 | Yes | Yes | Yes | Yes | Yes | Partial; workers and queues can be controlled, but not the same task-type enqueue-only model | Partial | Good | Partial; not the central model | Partial | Partial | General durable background jobs |
| db-scheduler | 7/10 | Yes | Yes | Yes | Yes | Yes | Partial | Partial | Good | Limited compared with Swarm | Partial | Partial | Lightweight embedded durable scheduling |
| Quartz + Spring Boot | 6.5/10 | Via triggers | Yes | Yes | Yes | Yes | Partial; jobs and triggers can be paused or resumed | Mostly custom | Mostly custom | Not native rate-limited task execution | Partial | Partial | Mature persistent scheduling |
| Temporal Java / Spring Boot | 6/10 | Yes | Yes | Yes | Yes | Yes | Different model via task queues and workers | Different workflow/activity failure model | Very strong | Possible, but different architecture | Worker/activity controls | Strong recovery, different model | Durable workflows and orchestration |
| Spring Batch | 5.5/10 | Job launch | Usually external scheduler | Usually external scheduler | Yes | Partial | Not the same model | Skip/reject concepts in batch processing | Strong | Not a task-type pacer | Batch-oriented | Restartability rather than task reclaim | ETL, chunk processing, restartable batch jobs |
| Spring Integration JDBC | 4/10 | As messages | Possible but assembled | Possible but assembled | Yes, as message persistence | Partial | Custom | Custom | Custom | Custom | Custom | Custom | Building blocks for message-driven flows |
| ShedLock | 2/10 | No | Only scheduled methods | Yes, via `@Scheduled` | No task queue | Prevents duplicate scheduled execution | No | No | No | No | No | No | Locking scheduled methods across nodes |

The closest broad durable background job alternative is JobRunr. The closest lightweight embedded scheduler alternative is db-scheduler. Quartz is mature and powerful, but scheduler-centric and heavier than mango-swarm's task-type execution model. Spring Batch is excellent for batch pipelines, but not the same shape as many small independently queued application tasks. Temporal is more powerful for workflows, but introduces a larger external workflow architecture. Spring Integration provides useful building blocks, but does not directly provide mango-swarm's task-type execution control model. ShedLock only solves distributed locking for scheduled methods.

## Why Not ShedLock?

ShedLock solves a narrower problem: coordinating scheduled jobs so only one application instance runs a given scheduled method at a time.

That is useful, but it does not provide a durable task queue.

For mango-swarm's target use case, ShedLock leaves the application to build most of the system itself:

- no durable per-task payload table
- no per-task lifecycle state
- no task claiming model
- no retry metadata per task
- no timeout reclaim for abandoned work
- no per-task progress visibility
- no per-task-type queue, execute, reject, or drop modes
- no backlog draining semantics
- no built-in rate division across active workers
- no local pacing to prevent catch-up bursts

With ShedLock, a common design is one locked scheduled method that scans a table and processes rows. That can work, but the application then owns row claiming, retries, timeouts, concurrency, rate limiting, progress, cleanup, and crash recovery.

`mango-swarm` makes those concerns first-class.

## Why Not Spring Batch?

Spring Batch is a mature framework for batch workloads. It is strong when the problem is naturally a batch job:

- read many records
- process them in chunks
- write outputs
- track job and step execution
- restart failed jobs
- model complex batch pipelines

That is a different shape from mango-swarm's target workload.

For many application task queues, Spring Batch is more framework than the problem needs. A task such as "send this email", "call this partner API", or "process this customer event after a delay" does not naturally need a job repository, step model, item reader, item processor, item writer, chunk boundaries, or batch restart graph.

`mango-swarm` is task-oriented:

- one durable row is one unit of work
- the payload is application JSON
- the handler is normal application code
- queue time is earliest eligibility
- workers claim eligible rows
- local token rings decide when execution may start
- retry and terminal state live on the task row
- progress lives in a narrow runtime row

This keeps the operational model smaller for services that need distributed background tasks, not batch pipelines.

## Why mango-swarm Is Better For This Use Case

For durable application tasks, mango-swarm has the right primitives built in.

### Durable Tasks

Tasks are persisted with JSON payloads. The durable task row records lifecycle state, claim metadata, attempts, retry timing, completion/failure timestamps, and the last error.

The application does not need to invent a queue and then bolt coordination rules onto it.

### Multi-Instance Coordination

Workers heartbeat into swarm state. Active worker count is used to divide configured task-type rates across the currently live application instances.

Each worker claims eligible tasks, so multiple instances can cooperate without a separate broker.

### Rate Limits That Survive Backlogs

Task-type rates are configured at application level. Each worker gets a local share:

```text
effectiveLocalRate = configuredRate / activeWorkerCount
```

Each worker enforces that share with a fixed-capacity local token ring per task type. The ring itself is reused, while the active token count changes with the worker's current local share. When more workers arrive, reduced share is enforced immediately by disabling excess token slots. When workers disappear, only the newly added token slots are enabled, and those slots are scheduled no earlier than the next configured period after reconfiguration. Existing active tokens keep their current windows, so recalculating the share does not create a fresh burst or add replacement capacity inside the current period.

That means a shrink followed by a grow inside the same period does not put removed tokens back into a period where some starts have already been spent. The higher local share takes effect from the next period onward.

This matters when many tasks are already eligible. `available_at` is only earliest database eligibility. It does not guarantee immediate execution. Even if thousands of rows are overdue, workers start tasks only when:

- global worker concurrency permits it
- task-type concurrency permits it
- the local token ring grants a valid token

Expired token windows are skipped, not replayed. That prevents the concertina effect where downtime or a paused queue turns into a burst when workers resume.

### Operational Task Modes

Task types can be controlled without deleting code:

| Mode | Producer behavior | Worker behavior |
| --- | --- | --- |
| `execute` | insert new task rows | claim and execute eligible rows |
| `queue` | insert new task rows | leave rows queued |
| `reject` | fail before insert | leave existing rows queued |
| `drop` | acknowledge without insert | leave existing rows queued |

This is useful when a dependency is down, a partner API is throttling, or a class of tasks must accumulate until it is safe to resume.

### Hot Runtime State Stays Separate

The durable task row contains the JSON payload and lifecycle state. It should not be updated for every progress message.

Frequent mutable execution state lives separately from the durable payload row.

This avoids turning large durable task rows into hot rows and keeps progress/liveness updates narrow.

### Simple Handler Model

Application code implements a `TaskHandler<T>`.

Handlers receive a `TaskExecutionContext<T>` with metadata, payload, and progress reporting:

```java
context.progress(30, "Calling partner API");
```

The handler does not need to know about database locking, worker heartbeats, retry bookkeeping, or token-ring pacing.

## When mango-swarm Fits Best

Use mango-swarm when a Spring Boot application can use PostgreSQL for its durable task store and needs embedded durable background work where task types can be queued, delayed, paused in queue-only mode, activated later, retried with task-specific backoff, dropped or rejected by policy, and executed under task-specific rate and concurrency limits without adopting a full workflow platform, external broker, or heavy batch framework.

It fits workloads with:

- many independent tasks
- immediate queueing from application code
- future scheduling with `at(...)` or `after(...)`
- configurable retry and backoff policies
- retry needs ranging from a few short retries to many retries over an extended period
- distributed execution across multiple app instances
- worker heartbeat and stale-worker detection
- timeout reclaim for idempotent abandoned work
- per-task-type rate and concurrency limits
- smooth local pacing to avoid backlog bursts
- task-type modes for `execute`, `queue`, `reject`, and `drop`
- progress or liveness reporting where useful
- Spring Boot auto-configuration

## When Another Tool May Be Better

Use Spring Batch for structured batch/ETL pipelines, chunk processing, restartable jobs, and large data processing workflows.

Use Quartz when the main problem is rich calendar or cron scheduling.

Use JobRunr when a ready-made general-purpose background job system with dashboard support is desired.

Use db-scheduler when the main requirement is a small embedded persistent scheduler.

Use Temporal when the problem is long-running durable workflow orchestration, sagas, human-in-the-loop flows, or cross-service workflows.

Use Spring Integration JDBC-backed channels or message stores when the application needs message-driven integration building blocks and is prepared to assemble task execution policy itself.

Use ShedLock when the only requirement is preventing duplicate execution of `@Scheduled` methods across instances.

In short: ShedLock coordinates scheduled methods, Spring Batch models batch jobs, Quartz and db-scheduler schedule jobs, Temporal orchestrates workflows, and mango-swarm runs durable distributed application tasks under task-type execution policy.
