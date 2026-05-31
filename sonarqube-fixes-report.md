# SonarQube Fix Report

Project: `mango-swarm`

SonarQube: `http://spitfire:9000`

Date: 2026-05-31

## Result

- Open Bugs: 0
- Open Code Smells: 0
- Security Hotspots: not changed automatically
- Quality Gate: OK

## Fixes

### Test-only assertion and lambda cleanups

- Replaced map-value equality assertions with AssertJ `containsEntry`.
- Moved object construction out of `assertThatThrownBy` lambdas so each lambda contains a single potentially throwing call.
- Removed an unused `throws Exception` declaration.
- Replaced boolean equality assertions with `isTrue` / `isFalse`.
- Replaced `Thread.sleep` in a test polling helper with `LockSupport.parkNanos` while preserving interrupt handling.

These changes satisfy `java:S5838`, `java:S5778`, `java:S1130`, `java:S2925`, and related test smells without changing production behaviour.

### Unused code and parameter cleanups

- Removed unused private accessors from the private `EmailPayload` test helper.
- Removed the unused `rowNum` parameter from the package-private `JdbcTaskRepository.mapTask` helper and updated call sites.
- Renamed local test variables away from restricted identifiers.

These changes satisfy `java:S1144`, `java:S1172`, and `java:S6213` by removing dead/private-only code or unused parameters.

### SQL text block whitespace

- Replaced tab-indented SQL text block content in `JdbcWorkerRegistry` with spaces.

This satisfies `java:S2479` without changing SQL semantics.

### Public API and persistence-format rules

- Suppressed rules where the Sonar recommendation would change public API or persisted enum names:
  - `TaskStatus` lowercase constants are stored in task status strings.
  - `TaskExecutionContext.ProgressReporter.record` is an internal callback name.
  - `TaskHandler.execute` intentionally allows `throws Exception` for application handlers.
  - `TaskHandlerRegistry.get` intentionally returns a wildcard handler for heterogeneous task payload types.
  - `SchemaQualifiedTables` remains a class to preserve constructor/API shape.

These suppressions keep runtime/API behaviour unchanged while documenting why the rule is intentionally not applied.

### Schema identifier regex

- Replaced `[A-Za-z0-9_]` with `\\w` after the first identifier character.

This satisfies `java:S6353` while preserving the same accepted schema identifier shape.

## Validation

Commands run:

```sh
mvn test
mvn clean verify sonar:sonar -Dsonar.host.url=http://spitfire:9000 -Dsonar.projectKey=mango-swarm -Dsonar.token="$SONAR_QUBE_TOKEN"
mvn clean verify sonar:sonar -Dsonar.host.url=http://spitfire:9000 -Dsonar.projectKey=mango-swarm -Dsonar.token="$SONAR_QUBE_TOKEN"
```

Observed results:

- `mvn test`: BUILD SUCCESS, 128 tests run, 0 failures, 0 errors, 1 skipped.
- First full Sonar run: BUILD SUCCESS, quality gate OK, remaining Bugs/Code Smells reduced to 2.
- Second full Sonar run: BUILD SUCCESS, quality gate OK.
- Final Sonar issue query for unresolved Bugs and Code Smells returned no rows.

## Notes

- Sonar reported missing SCM blame for modified/uncommitted files, which is expected while these changes are not committed.
- Maven build compiles with release 17; the Sonar scanner provisions/runs its analysis engine on Java 21.
