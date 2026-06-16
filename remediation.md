# Security Remediation Notes

Scope: production code under `src/main/java`. The reference email example app is intentionally excluded.

## 1. Persisted and Logged Failure Messages May Leak Sensitive Data

### Where
- [MangoSwarmDaemon.java](/home/jallen/git/mango4j/mango4j-swarm/src/main/java/ie/bitstep/mango/swarm/executor/MangoSwarmDaemon.java#L494)
- [MangoSwarmDaemon.java](/home/jallen/git/mango4j/mango4j-swarm/src/main/java/ie/bitstep/mango/swarm/executor/MangoSwarmDaemon.java#L506)
- [JdbcTaskRepository.java](/home/jallen/git/mango4j/mango4j-swarm/src/main/java/ie/bitstep/mango/swarm/db/JdbcTaskRepository.java#L377)

### Issue
When task execution fails, the daemon forwards `ex.getMessage()` into task-retry and failure handling. That value is then persisted to `last_error_message` and may also be emitted in debug logs alongside the exception object.

This is a data-exposure risk if handler exceptions include:
- credentials or API tokens
- customer data from payloads
- SQL fragments or upstream service responses
- stack traces or nested exception messages with sensitive context

Because the message is stored durably, the exposure can outlive the original request and be accessible to operators, support tooling, backups, and downstream analytics.

### Suggested Fix
- Introduce a sanitization step before persisting any failure message.
- Prefer a short, generic user-facing classification such as `task failed` or `upstream service error`.
- Keep the full exception for internal diagnostics only when explicitly enabled, and avoid logging raw exception messages at normal verbosity.
- If detailed diagnostics are needed, store them in a separate opt-in field or structured audit channel with access controls.

### Practical Remediation Pattern
1. Add a helper that truncates and redacts failure text before storage.
2. Pass the sanitized string to `rescheduleAfterFailure(...)` and `markFailed(...)`.
3. Log only the task id, task type, and a stable failure code by default.
4. Keep stack traces behind debug logs or remove them from routine paths entirely.

### Verification
- Add a test that simulates an exception message containing a token-like string and verifies the persisted message is redacted.
- Add a test that ensures failure logging does not emit the raw exception message at info/warn level.

## 2. No Other High-Risk Core Issues Identified in This Review

I did not find:
- SQL string concatenation in the repository layer
- command execution paths
- unsafe Jackson default typing / polymorphic deserialization setup
- dynamic class loading based on task payloads

That does not remove the need for regular review of application-specific task handlers, but the core library paths reviewed here look structurally sound on those dimensions.
