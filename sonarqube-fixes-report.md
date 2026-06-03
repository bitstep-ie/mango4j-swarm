# SonarQube Fix Report

Project: `mango-swarm`

SonarQube: `http://spitfire:9000`

Date: 2026-06-01

## Result

- Quality Gate: OK
- Open Bugs: 0
- Open Code Smells: 0
- Security Hotspots: 0
- Overall coverage: 93.7%
- New coverage: 87.8%
- Duplication: 0.0%

## Fixes

### Runtime State Split

- Added `mango_swarm_task_runtime` for mutable execution state and progress.
- Removed progress fields from the durable task row.
- Kept task-row updates focused on lifecycle transitions: claim, running, complete, fail, retry, and reclaim.
- Added PostgreSQL `fillfactor = 75` for the runtime table.
- Avoided indexes on mutable runtime fields; only `worker_id, task_id` is indexed for operational lookup.

This addresses the hot-row and table-bloat risk by moving frequent progress/state writes away from payload-bearing task rows.

### Runtime Progress API

- Kept `TaskExecutionContext.progress(int)` and `TaskExecutionContext.progress(int, String)` as the public progress API.
- Kept progress persistence routing internal to `TaskExecutionContext`.
- Extended repository runtime writes with `TaskRepository.updateRuntime(...)`.

This preserves existing handler behavior while allowing optional execution-state and progress reporting.

### Progress Write Throttling

- Added runtime progress settings:
  - `progressThresholdPercent`, default `10`.
  - `minUpdateInterval`, default `30s`.
- Runtime persistence now occurs only when state changes, message changes, progress changes by the configured threshold, or the minimum interval has elapsed.

This limits database update churn while preserving visibility for meaningful progress changes.

### Timeout Liveness

- Timeout reclaim/fail queries now use `COALESCE(runtime.updated_at, task.claimed_at)`.
- Existing behavior is preserved for tasks without runtime rows.
- Progress updates keep runtime `updated_at` fresh without touching the durable task row.

This keeps timeout recovery behavior intact while moving high-frequency liveness writes to the narrow runtime table.

### Test And Mutation Coverage

- Added repository tests for runtime upsert, in-progress, failed, completion, retry, and requeue cleanup behavior.
- Added daemon tests for runtime progress throttling and threshold boundaries.
- Updated H2 schema/test support to match the runtime table split.

These tests protect the new persistence split and keep PIT above the configured threshold.

## Validation

Commands run:

```sh
mvn test -Dtest=TaskRepositoryTest,MangoSwarmDaemonTest
mvn spotless:check
mvn -P hammer-time
mvn clean verify sonar:sonar -Dsonar.host.url=http://spitfire:9000 -Dsonar.projectKey=mango-swarm -Dsonar.token="$SONAR_QUBE_TOKEN"
```

Observed results:

- Focused tests: BUILD SUCCESS, 68 tests run, 0 failures, 0 errors, 1 skipped.
- Repository tests after coverage additions: BUILD SUCCESS, 38 tests run, 0 failures, 0 errors, 1 skipped.
- `mvn -P hammer-time`: BUILD SUCCESS.
- PIT: 585 mutations generated, 526 killed, mutation score 90%, test strength 91%.
- PIT line coverage for mutated classes: 1120/1158, 97%.
- Sonar analysis: BUILD SUCCESS, compute-engine task `SUCCESS`.
- Sonar quality gate: OK.
- Sonar unresolved Bugs and Code Smells query returned `0`.

## Notes

- Security Hotspots were not fixed automatically.
- Sonar reported missing SCM blame for 10 modified/uncommitted files, which is expected before committing.
- Maven compilation uses release 17; Sonar scanner provisioned and ran its analysis engine on Java 21.
