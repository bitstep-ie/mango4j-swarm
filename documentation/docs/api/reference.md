# API Reference

This page documents the Java types application code interacts with directly: queuing work, implementing handlers, and reading execution context. For `mango4j.swarm.*` configuration properties, see the [README Configuration Reference](https://github.com/bitstep-ie/mango4j-swarm#configuration-reference). For table/column details, see the [Schema reference](../schema/schema.md).

All types below live under the `ie.bitstep.mango.swarm` package (or a named subpackage where noted).

## `MangoTasks`

Injectable Spring bean used to queue work. All queuing methods return the persisted task's `UUID`, except under `mode: drop`, where an acknowledgement id is returned without persisting anything.

| Method | Parameters | Notes |
| --- | --- | --- |
| `queue(String taskType, JsonNode payload)` | task type key, JSON payload | Queues for immediate eligibility (`available_at = now`). |
| `queue(String taskType, Object payload)` | task type key, arbitrary object | Payload is converted via Jackson (`ObjectMapper.valueToTree`). |
| `at(Instant at, String taskType, JsonNode payload)` | target instant, task type key, JSON payload | Queues for eligibility at or after `at`. |
| `at(Instant at, String taskType, Object payload)` | target instant, task type key, arbitrary object | Object overload of `at(...)`. |
| `after(Duration delay, String taskType, JsonNode payload)` | delay, task type key, JSON payload | Shorthand for `at(now + delay, ...)`. |
| `after(Duration delay, String taskType, Object payload)` | delay, task type key, arbitrary object | Object overload of `after(...)`. |

All methods null-check their arguments (`NullPointerException`). All methods also throw:

- `IllegalArgumentException` if `taskType` has no matching configured `mango4j.swarm.task-types.*` entry.
- `IllegalStateException` if the task type's `mode` is `reject`.

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

    UUID sendAt(EmailRequest request, Instant at) {
        return tasks.at(at, "send-email", request);
    }

    UUID sendAfter(EmailRequest request, Duration delay) {
        return tasks.after(delay, "send-email", request);
    }
}
```

## `handler.TaskHandler<T>`

Application-defined task logic. One handler bean per configured task type, discovered automatically as a Spring bean.

| Method | Returns | Notes |
| --- | --- | --- |
| `payloadExtractor()` | `PayloadExtractor<T>` | Converts the durable JSON payload into the handler's Java model. Called once per attempt, before `execute(...)`. |
| `execute(TaskExecutionContext<T> context)` | `TaskExecutionResult` | Runs one attempt. May throw `TaskHandlerException` (checked) or any unchecked exception — both are caught by the daemon and routed through the normal retry/failure path. |

Annotate the implementation with `@handler.SwarmHandler("task-type-key")` — this both declares which configured task type the handler serves and registers it as a Spring `@Component`.

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

Returning `null` from `execute(...)` is treated as `completed()` for backward compatibility, but new handlers should return `TaskExecutionResult.completed()` explicitly.

## `payload.PayloadExtractor<T>` and `payload.PayloadReader`

`PayloadExtractor<T>` is a `@FunctionalInterface` with a single method, `T extract(PayloadReader reader)`, used as `payloadExtractor()`'s return value. `PayloadReader` wraps the durable JSON payload and supports aliasing, defaults, and validation so a handler can evolve its payload shape over time without breaking already-queued rows:

| Method | Returns | Notes |
| --- | --- | --- |
| `required(Class<T> type, String primaryPath, String... aliases)` | `T` | Reads the first matching path; throws `PayloadExtractionException` if none are present. |
| `optional(Class<T> type, String primaryPath, String... aliases)` | `OptionalValue<T>` | Reads the first matching path, or empty if none are present. |

`OptionalValue<T>` (nested in `PayloadReader`):

| Method | Returns | Notes |
| --- | --- | --- |
| `orDefault(T defaultValue)` | `T` | Value if present, else the default. |
| `asOptional()` | `Optional<T>` | Raw optional. |
| `orElseThrow(String message)` | `T` | Value if present, else throws `PayloadExtractionException(message)`. |
| `validate(Predicate<T> predicate, String message)` | `OptionalValue<T>` | Throws `PayloadExtractionException` if present but invalid; returns `this` for chaining. |

Paths support dotted nesting (e.g. `"customer.id"`), and `aliases` let a field be renamed in code while still reading rows queued under the old field name.

## `TaskExecutionContext<T>`

Passed to `TaskHandler.execute(...)`. Carries attempt metadata, the extracted payload, and callbacks for progress reporting and self-rescheduling.

| Method | Returns | Notes |
| --- | --- | --- |
| `taskId()` | `UUID` | Persisted task id (`mango_swarm_tasks.id`). |
| `taskType()` | `String` | Configured task type key. |
| `workerId()` | `UUID` | Worker id executing this attempt. |
| `attemptCount()` | `int` | Current attempt number. |
| `claimedAt()` | `Instant` | Claim timestamp for this attempt. |
| `payload()` | `T` | Extracted, typed payload. |
| `seriesId()` | `UUID` | Id shared by every occurrence of this task's recurring series, or `null` if standalone. See [Recurring tasks](#recurring-tasks-with-again) below. |
| `progress(int percent)` | `void` | Records progress `[0,100]`; also acts as liveness for timeout reclaim. |
| `progress(int percent, String description)` | `void` | Same, with a human-readable stage description. |
| `again(Duration delay)` | `UUID` | Queues a follow-up occurrence reusing the current payload. See below. |
| `again(Duration delay, T newPayload)` | `UUID` | Queues a follow-up occurrence with a new payload. See below. |

`progress(...)` throws `IllegalArgumentException` if `percent` is outside `[0,100]`.

### Recurring tasks with `again(...)`

A handler can queue its own follow-up occurrence from inside `execute(...)`, independent of the `TaskExecutionResult` it ultimately returns — call it on success, on failure, or both:

```java
@Override
public TaskExecutionResult execute(TaskExecutionContext<PollPayload> context) {
    PollPayload payload = context.payload();
    // ... do the work for this occurrence ...
    context.again(Duration.ofMinutes(10));                       // same payload
    // or: context.again(Duration.ofMinutes(10), payload.withCursor(next));
    return TaskExecutionResult.completed();
}
```

!!! note
    `again(...)` executes synchronously and immediately — it inserts a new task row the same way `MangoTasks.after(...)` does. It does not defer, batch, or affect the outcome recorded for the *current* attempt, and it respects the task type's `mode`/`wake-on-queue` settings exactly like `queue`/`at`/`after` (throws on `reject`, silently no-ops on `drop`).

Each occurrence is its own durable row, so occurrence history is free via the existing `cleanup.completed-retention`/`failed-retention` policy — no separate history table. Occurrences are linked via `mango_swarm_tasks.series_id`: the first occurrence created by `again()` gets `series_id` set to the *root* task's own `id`; every later occurrence inherits that same `series_id`, and the root task itself keeps `series_id = NULL`. To fetch a whole series: `WHERE id = :rootId OR series_id = :rootId`.

## `TaskExecutionResult`

Sealed interface returned by `execute(...)`.

| Factory | Returns | Notes |
| --- | --- | --- |
| `TaskExecutionResult.completed()` | `Completed` | Successful execution. |
| `TaskExecutionResult.failed(String message)` | `Failed` | Explicit failure; `message` is persisted for diagnostics and triggers the retry/failure flow based on task-type configuration. |

```java
if (partnerApiRejected) {
    return TaskExecutionResult.failed("Partner API returned 4xx");
}
return TaskExecutionResult.completed();
```
