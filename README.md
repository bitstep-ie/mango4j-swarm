# mango-swarm

Distributed PostgreSQL-backed, rate-limited task execution for Spring Boot applications.

<br/>

<div align="center">
  <picture>
    <source srcset="documentation/docs/assets/mango-with-text-black.png" media="(prefers-color-scheme: light)">
    <source srcset="documentation/docs/assets/mango-with-text-white.png" media="(prefers-color-scheme: dark)">
    <img src="documentation/docs/assets/mango-with-text-black.png" alt="mango Logo">
  </picture>

  <h3 align="center">mango-swarm</h3>

  <p align="center">
    Durable JSON tasks, self-coordinating workers, smooth rate limits, retries, and PostgreSQL-safe claiming.
    <br/><br/>
    <a href="documentation/how-mango-swarm-works.md"><strong>How It Works</strong></a>
    <br/><br/>
    <a href="examples/reference-email-app"><strong>Reference Email App</strong></a>
  </p>
</div>

<br/>

> **DRAFT**
>
> mango-swarm is currently under active design and implementation. APIs, configuration properties, table names, and migration structure may change before the first stable release.

# Introduction

**mango-swarm** is a small Spring Boot library for running background tasks across one or more instances of the same application.

It is designed for the common case where an application already owns a dedicated PostgreSQL database and needs durable, rate-limited work execution without introducing a separate broker.

The library provides:

* PostgreSQL-backed durable task storage
* JSONB payloads with payload evolution helpers
* worker registration and heartbeat pruning
* distributed rate division across active instances
* smooth time-slot based pacing instead of bursty permit release
* portable batch claiming for PostgreSQL and H2-backed tests
* per-task-type concurrency limits
* configurable global worker pool
* reserved virtual-thread configuration while keeping a Java 17 baseline
* timeout reclaim for idempotent task types
* exponential retry scheduling using the task table itself
* Spring Boot auto-configuration

mango-swarm assumes all running instances belong to the same application and support the same configured task types. It is not intended as a shared cross-application task bus.

***

# Quick Start

Add the library:

```xml
<dependency>
    <groupId>ie.bitstep.mango</groupId>
    <artifactId>mango-swarm</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Configure a task type:

```yaml
mango4j:
  swarm:
    enabled: true

    worker:
      heartbeat-interval: 10s
      stale-after: 30s

    executor:
      max-threads: 16
      poll-interval: 100ms
      virtual-threads: auto

    retry:
      base-delay: 5s
      multiplier: 2.0
      max-delay: 30m

    task-types:
      send-email:
        mode: execute
        rate: 100
        period: 1s
        concurrency: 5
        timeout: 30s
        reclaim-on-timeout: true
        idempotent: true
        max-attempts: 3
```

Queue work from application code:

```java
@Service
class EmailTaskService {
    private final MangoTasks tasks;

    EmailTaskService(MangoTasks tasks) {
        this.tasks = tasks;
    }

    UUID sendNow(EmailRequest request) {
        return tasks.queue("send-email", request);
    }

    UUID sendLater(EmailRequest request, Instant at) {
        return tasks.at(at, "send-email", request);
    }

    UUID sendAfter(EmailRequest request, Duration delay) {
        return tasks.after(delay, "send-email", request);
    }
}
```

Implement a handler:

```java
@SwarmHandler("send-email")
class SendEmailTaskHandler implements TaskHandler<EmailPayload> {

    @Override
    public PayloadExtractor<EmailPayload> payloadExtractor() {
        return reader -> new EmailPayload(
                reader.required(String.class, "customerId", "userId", "customer.id"),
                reader.required(String.class, "to", "email", "recipientEmail", "to.address"),
                reader.optional(String.class, "subject").orDefault("Hello"),
                reader.optional(String.class, "body").orDefault(""));
    }

    @Override
    public TaskExecutionResult execute(TaskExecutionContext<EmailPayload> context) {
        EmailPayload payload = context.payload();
        context.progress(10, "connecting");
        // send email, call partner API, write a report, etc.
        return TaskExecutionResult.completed();
    }
}
```

On startup, mango-swarm discovers all `TaskHandler` beans, verifies the configured task types, starts a worker heartbeat, and begins claiming available tasks.

Handlers receive a `TaskExecutionContext<T>` containing:

- `taskId`: persisted task UUID from `mango_swarm_tasks.id`
- `taskType`: configured task type key
- `workerId`: worker UUID executing the attempt
- `attemptCount`: current attempt number
- `claimedAt`: claim timestamp for the current attempt
- `payload`: extracted typed payload
- progress API: `progress(percent)` and `progress(percent, description)`

Calling `context.progress(percent)` or `context.progress(percent, description)` records progress from `0` to `100` in the runtime table and also acts as task liveness. Runtime updates extend the stale-task timeout window for that task and refresh the current `execution_time_ms`. The optional description can be used for human-readable stages such as `connecting`, `sending`, or `completing`.

When a task completes successfully, mango-swarm records a final progress state automatically:

```text
execution_state = completed
progress_percent = 100
progress_message = finished
execution_time_ms = elapsed attempt time in milliseconds
```

Handlers can still report `100` themselves if they want a custom final description before returning, but the default successful completion state is always `100` / `finished`.

Handlers should return `TaskExecutionResult.completed()` for success or `TaskExecutionResult.failed(message)` for an explicit failure. A `null` result is still treated as success for compatibility, but new handlers should not rely on that behavior.

***

# Configuration Reference

All configuration is under `mango4j.swarm`.

## Full Example

```yaml
mango4j:
  swarm:
    enabled: true
    allow-unconfigured-handlers: false

    database:
      schema: application_schema
      apply-schema-to-hibernate-default: true

    worker:
      heartbeat-interval: 10s
      stale-after: 30s

    cleanup:
      enabled: true
      interval: 10m
      completed-retention: 30d
      failed-retention: 90d
      pacer-retention: 30d
      batch-size: 1000

    executor:
      max-threads: auto        # auto or explicit integer string (e.g. "16")
      poll-interval: 100ms
      queue-strategy: CALLER_RUNS   # CALLER_RUNS | ABORT
      virtual-threads: auto    # auto | enabled | disabled

    retry:
      base-delay: 5s
      multiplier: 2.0
      max-delay: 30m

    task-types:
      send-email:
        mode: execute
        rate: 100
        period: 1s
        concurrency: 5
        timeout: 30s
        reclaim-on-timeout: true
        idempotent: true
        batch-size: 20
        max-attempts: 3
        retry-base-delay: 10s
        retry-multiplier: 3.0
        retry-max-delay: 10m
        # retry-delay: 10s   # backward-compatible alias for first retry delay
```

## Property List

### Root

* `mango4j.swarm.enabled` (default `true`): enables the library.
* `mango4j.swarm.allow-unconfigured-handlers` (default `false`): when `false`, startup fails if a discovered handler has no matching configured task type.

### Database

* `mango4j.swarm.database.schema` (optional): schema prefix for native SQL table access.
* `mango4j.swarm.database.apply-schema-to-hibernate-default` (default `true`): if `true` and `hibernate.default_schema` is not set, swarm can set it to `database.schema`.

### Worker

* `mango4j.swarm.worker.heartbeat-interval` (default `10s`): worker heartbeat cadence.
* `mango4j.swarm.worker.stale-after` (default `30s`): worker considered stale after this silence window.

### Cleanup

* `mango4j.swarm.cleanup.enabled` (default `true`): enables periodic cleanup of terminal tasks.
* `mango4j.swarm.cleanup.interval` (default `10m`): cleanup run cadence.
* `mango4j.swarm.cleanup.completed-retention` (default `30d`): completed tasks older than this are deleted.
* `mango4j.swarm.cleanup.failed-retention` (default `90d`): failed tasks older than this are deleted.
* `mango4j.swarm.cleanup.pacer-retention` (default `30d`): task pacing slots older than this are deleted.
* `mango4j.swarm.cleanup.batch-size` (default `1000`): maximum rows deleted per cleanup category per pass.

## Cleanup Task

mango-swarm runs a built-in periodic cleanup task in the daemon loop (no separate handler needed).

Behavior:

* cleanup runs every `mango4j.swarm.cleanup.interval`
* completed rows are deleted when `completed_at < now - completed-retention`
* failed rows are deleted when `failed_at < now - failed-retention`
* task pacing slots are deleted when `slot_at < now - pacer-retention`
* each cleanup category deletes at most `batch-size` rows per pass
* only terminal states are deleted (`completed`, `failed`)
* `queued`, `claimed`, and `in_progress` rows are never removed by cleanup

Defaults:

* `completed-retention: 30d`
* `failed-retention: 90d`
* `pacer-retention: 30d`
* `batch-size: 1000`

Disable cleanup entirely:

```yaml
mango4j:
  swarm:
    cleanup:
      enabled: false
```

Tune cleanup cadence/retention:

```yaml
mango4j:
  swarm:
    cleanup:
      interval: 15m
      completed-retention: 14d
      failed-retention: 60d
      pacer-retention: 14d
      batch-size: 500
```

### Executor

* `mango4j.swarm.executor.max-threads` (default `auto`): global local execution cap.
* `mango4j.swarm.executor.poll-interval` (default `100ms`): fallback poll delay when no rate-gated wakeup applies.
* `mango4j.swarm.executor.queue-strategy` (default `CALLER_RUNS`): overload behavior (`CALLER_RUNS` or `ABORT`).
* `mango4j.swarm.executor.virtual-threads` (default `auto`): reserved virtual-thread policy (`auto`, `enabled`, `disabled`). Current Java 17 builds always use platform threads.

### Retry Defaults

* `mango4j.swarm.retry.base-delay` (default `0s`)
* `mango4j.swarm.retry.multiplier` (default `2.0`)
* `mango4j.swarm.retry.max-delay` (default `30m`)

### Per Task Type (`mango4j.swarm.task-types.<task-type>.*`)

Required:
* `rate`: permits per period for the whole application.

Optional:
* `mode` (default `execute`): controls queueing and execution for this task type.
* `period` (default `1s`)
* `concurrency` (default `1`)
* `timeout` (default `1m`)
* `reclaim-on-timeout` (default `false`)
* `idempotent` (default `false`)
* `batch-size` (derived when omitted)
* `max-attempts` (default `1`)
* `retry-base-delay` (override)
* `retry-multiplier` (override)
* `retry-max-delay` (override)
* `retry-delay` (legacy alias for first retry delay)

Task type modes:

| Mode | New queue attempts | Existing queued rows | Timeout recovery |
| --- | --- | --- | --- |
| `execute` | inserted | claimed and executed | active |
| `queue` | inserted | not claimed | skipped |
| `reject` | throws an error before insert | not claimed | skipped |
| `drop` | returns an acknowledgement id without insert | not claimed | skipped |

## Minimal Example

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: execute
        rate: 20
```

Queue tasks without executing them:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: queue
        rate: 20
```

Reject new tasks and keep existing rows from being claimed:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: reject
        rate: 20
```

Drop new tasks without raising an error and keep existing rows from being claimed:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: drop
        rate: 20
```

## Recommended Production Baseline

```yaml
mango4j:
  swarm:
    worker:
      heartbeat-interval: 10s
      stale-after: 30s
    cleanup:
      interval: 10m
      completed-retention: 30d
      failed-retention: 90d
      pacer-retention: 30d
      batch-size: 1000
    executor:
      max-threads: "16"
      poll-interval: 100ms
      virtual-threads: auto
    retry:
      base-delay: 5s
      multiplier: 2.0
      max-delay: 30m
    task-types:
      send-email:
        mode: execute
        rate: 100
        period: 1s
        concurrency: 5
        timeout: 30s
        reclaim-on-timeout: true
        idempotent: true
        max-attempts: 3
```

***

# Scheduling

`MangoTasks` exposes three scheduling methods:

```java
tasks.queue("send-email", payload);                 // schedule as soon as the next slot allows
tasks.at(instant, "send-email", payload);           // schedule at or after a requested instant
tasks.after(duration, "send-email", payload);       // schedule after a relative delay
```

Scheduling uses a per-task-type pacer table. This prevents a task scheduled far in the future from blocking tasks that should run before it.

When a task is queued:

* mango-swarm looks at the closest occupied slot before the requested time
* it looks at the closest occupied slot after the requested time
* if the requested time is too close to the slot before, the task moves after that before slot
* if the requested time is too close to the slot after, the task moves after that after slot
* otherwise the requested time is used

In short: a requested time is pushed forward only when it would collide with a nearby existing execution slot. Future slots do not reserve the whole timeline before them.

***

# Distributed Rate Limits

Rates are configured at application level and divided by the number of active workers.

If `send-email` is configured for `100` tasks per second and there are `4` active workers, each worker aims for roughly `25` tasks per second.

Workers register themselves with a random UUID and send periodic heartbeats to PostgreSQL. Each heartbeat also prunes stale workers. The active worker count is used to recalculate local rates.

Execution is paced smoothly across the configured period. mango-swarm uses time slots instead of releasing all permits at once, so `100/sec` means work is spread through the second rather than claimed in one burst.

***

# Batch Claiming

Task acquisition is batch-based for throughput.

For each polling cycle, a worker calculates how many tasks it can claim from:

* the effective local rate
* the configured or derived batch size
* remaining task-type concurrency
* remaining global executor capacity

The claim limit is conceptually:

```text
min(
  configured or derived batch size,
  remaining local rate capacity,
  remaining task-type concurrency capacity,
  remaining executor capacity
)
```

If no `batch-size` is configured, mango-swarm derives one from the effective local rate, concurrency, and executor capacity.

Database claiming uses a portable select-and-update flow so the repository can run against PostgreSQL and the H2-backed test suite. PostgreSQL-specific concurrent claim locking is skipped in H2 validation.

***

# Thread Pool And Concurrency

The global executor pool is configured independently from task-type concurrency.

```yaml
mango4j:
  swarm:
    executor:
      max-threads: 16
      virtual-threads: auto
```

Task-type concurrency limits how many tasks of that type can run on one worker. The global executor limit controls total local execution capacity.

For example, four task types with concurrency `5` each cannot all run at full concurrency if the global executor has only `5` threads.

Java 17 is the compile baseline. The `virtual-threads` setting is reserved for future Java 21+ runtime support; current builds always use platform threads. The default cap is conservative, so set `max-threads` explicitly for predictable production behavior.

***

# Retries And Reclaim

Handler failures are retried by rescheduling the same task row:

* if `attempt_count < max-attempts`, the task returns to `queued`
* `available_at` is set to the next retry time
* the retry delay uses exponential backoff
* after the final attempt, the task is marked `failed`

Backoff can be configured globally and overridden per task type:

```yaml
mango4j:
  swarm:
    retry:
      base-delay: 5s
      multiplier: 2.0
      max-delay: 30m
    task-types:
      send-email:
        mode: execute
        max-attempts: 3
        retry-base-delay: 10s
        retry-multiplier: 3.0
        retry-max-delay: 10m
```

Timeout reclaim is separate from handler failure retry. A task whose liveness timestamp exceeds its configured timeout is only requeued when both are true:

* `reclaim-on-timeout: true`
* `idempotent: true`

Any task type that allows reclaim must be idempotent. If reclaim is disabled, timed-out tasks are marked failed according to the configured timeout policy.

For reclaim purposes, `timeout` means maximum allowed silence from the task, not maximum total runtime. Timeout detection uses task liveness, not just the original claim time.

When a handler calls:

```java
context.progress(50, "sending");
```

mango-swarm updates `progress_percent`, `progress_message`, `updated_at`, and `execution_time_ms` on `mango_swarm_task_runtime`. Timeout recovery checks runtime `updated_at` when present, otherwise it falls back to the task row's `claimed_at`.

For example, with `timeout: 30s`:

* if a task is claimed at `10:00:00` and never reports progress, it can be treated as stale after about `10:00:30`
* if it reports progress at `10:00:20`, it is not stale until about `10:00:50`
* if it reports progress again at `10:00:45`, it is not stale until about `10:01:15`

This means handlers are expected to call `progress(percent, description)` periodically while doing long-running work. A progress call does not need to mean the exact amount of business work completed, but it must represent real handler liveness. The stored percentage and message are useful for observability; the liveness extension comes from the runtime update timestamp.

***

# Payload Evolution

Task payloads are stored as PostgreSQL `jsonb`. Handlers do not deserialize directly into the current Java payload class.

Instead:

```text
jsonb -> JsonNode -> PayloadExtractor -> current Java payload object
```

This lets old durable payloads survive Java model changes.

```java
reader.required(String.class, "customerId", "userId", "customer.id");

reader.required(String.class, "to", "email", "recipientEmail", "to.address");

reader.optional(String.class, "templateId", "template.id", "emailTemplate")
        .orDefault("default-template");

reader.optional(Integer.class, "priority")
        .orDefault(5);
```

Required fields fail with a clear extraction error if the semantic value cannot be derived. Optional fields can use aliases, defaults, validation, or `Optional<T>`.

***

# Database

mango-swarm requires four tables:

* `mango_swarm_workers`
* `mango_swarm_tasks`
* `mango_swarm_task_runtime`
* `mango_swarm_task_pacers`

Task IDs are Java-generated UUIDv7 values.

Applications own schema creation and schema selection:

```yaml
spring:
  flyway:
    init-sqls: CREATE SCHEMA IF NOT EXISTS application_schema
    default-schema: application_schema
    schemas: application_schema

mango4j:
  swarm:
    database:
      schema: application_schema
```

For JPA applications, set Hibernate's default schema when the swarm tables should live in the application schema:

```yaml
spring:
  jpa:
    properties:
      hibernate.default_schema: application_schema
```

`mango4j.swarm.database.schema` is used for native SQL. JPA entity mappings, where used by an application, should rely on Hibernate's `default_schema` rather than hard-coded `@Table(schema = ...)` values.

The library does not ship a runtime migration in its jar. Application teams should create and manage these tables in their own schema/migration pipeline.

A reference DDL is provided in:

```text
documentation/mango-swarm-schema.sql
```

***

# Reference App

A runnable Spring Boot example lives in:

```text
examples/reference-email-app
```

It creates a `send-email` task type, queues sample tasks on startup, schedules one future task, and logs each request when the handler fires.

Run PostgreSQL:

```bash
docker run --rm --name mango-reference-postgres \
  -e POSTGRES_DB=mango_reference \
  -e POSTGRES_USER=mango \
  -e POSTGRES_PASSWORD=mango \
  -p 5432:5432 postgres:16-alpine
```

Install the library:

```bash
mvn clean install
```

Run the app:

```bash
cd examples/reference-email-app
mvn spring-boot:run
```

***

# Development

Run the test suite:

```bash
mvn test
```

Integration tests use an in-memory H2 database in PostgreSQL compatibility mode. PostgreSQL-specific row-lock concurrency behavior is not run in the H2 suite.

***
