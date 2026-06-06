# Why mango-swarm?

`mango-swarm` exists for a specific application problem: durable, rate-limited background task execution inside a Spring Boot service that already owns PostgreSQL.

It is not trying to be a general job-processing platform, a workflow engine, or a replacement for every scheduler. It is built for application-owned work such as:

- sending email or notifications
- calling third-party APIs under strict rate limits
- processing delayed or retryable application tasks
- running many independent units of work across multiple service instances
- keeping queued work durable without adding a broker
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

Tasks are stored in PostgreSQL with JSON payloads. The task row records lifecycle state, claim metadata, attempts, retry timing, completion/failure timestamps, and the last error.

The application does not need to invent a queue table and then bolt coordination rules onto it.

### Multi-Instance Coordination

Workers heartbeat into `mango_swarm_workers`. Active worker count is used to divide configured task-type rates across the currently live application instances.

Each worker claims eligible tasks from `mango_swarm_tasks`, so multiple instances can cooperate without a separate broker.

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

### PostgreSQL-Friendly Runtime State

The durable task row contains the JSON payload and lifecycle state. It should not be updated for every progress message.

Frequent mutable execution state lives in `mango_swarm_task_runtime`, a narrow table without `jsonb` and with PostgreSQL-friendly `fillfactor`.

This avoids turning large durable task rows into hot rows and reduces MVCC bloat, dead tuples, index churn, and vacuum pressure under high progress update rates.

### Simple Handler Model

Application code implements a `TaskHandler<T>`.

Handlers receive a `TaskExecutionContext<T>` with metadata, payload, and progress reporting:

```java
context.progress(30, "Calling partner API");
```

The handler does not need to know about database locking, worker heartbeats, retry bookkeeping, or token-ring pacing.

## When ShedLock Is A Better Fit

Use ShedLock when the problem is simply:

- "run this scheduled method on only one instance"
- no durable per-item queue is needed
- no per-task retry/progress/lifecycle state is needed
- no backlog draining semantics are needed

Examples:

- once-per-day cleanup job
- periodic cache refresh
- scheduled aggregate rebuild where only one runner is allowed

## When Spring Batch Is A Better Fit

Use Spring Batch when the workload is a true batch pipeline:

- file or table readers
- chunk-oriented processing
- restartable job/step execution
- complex job flows
- item readers/processors/writers
- batch metadata and operational controls

Examples:

- nightly import of a large file
- ETL pipeline
- chunked migration of millions of rows
- multi-step financial settlement process

## When mango-swarm Is The Better Fit

Use mango-swarm when the workload is:

- many independent tasks
- queued from application code
- delayed or retryable
- distributed across multiple app instances
- rate-limited per task type
- sensitive to backlog bursts
- needs per-task progress or liveness
- should stay inside the application's PostgreSQL database
- does not need a separate broker or full batch framework

In short: ShedLock coordinates scheduled methods, Spring Batch models batch jobs, and mango-swarm runs durable distributed application tasks.
