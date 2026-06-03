# Reference Email App

This is a minimal Spring Boot app that uses `mango-swarm` to queue and execute `send-email` tasks.

The handler does not send real email. It logs the request when the task fires.
It also reports progress stages (for example `preparing`) and the library writes a final `100 / finished` progress state when the task completes.

## Schema Ownership

The swarm library does not ship a runtime migration. The application owns schema creation, table creation, and schema selection.

This reference app does that in `application.yml`:

```yaml
spring:
  flyway:
    init-sqls: CREATE SCHEMA IF NOT EXISTS mango_reference
    default-schema: mango_reference
    schemas: mango_reference

mango4j:
  swarm:
    database:
      schema: mango_reference
```

For JPA applications, set Hibernate's default schema too:

```yaml
spring:
  jpa:
    properties:
      hibernate.default_schema: mango_reference
```

## Running

Start PostgreSQL locally:

```bash
docker run --rm --name mango-reference-postgres \
  -e POSTGRES_DB=mango_reference \
  -e POSTGRES_USER=mango \
  -e POSTGRES_PASSWORD=mango \
  -p 5432:5432 postgres:16-alpine
```

Install the library from the repository root:

```bash
mvn clean install
```

Run the reference app:

```bash
cd examples/reference-email-app
mvn spring-boot:run
```

On startup the app queues the configured number of immediate `send-email` tasks and one extra task scheduled 30 seconds in the future. The swarm daemon claims tasks when they become available, and the handler logs each request when it fires.

The reference app config uses `mode: execute`, which is the normal mode for accepting and executing tasks:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: execute
```

Task type modes:

| Mode | New queue attempts | Existing queued rows |
| --- | --- | --- |
| `execute` | inserted | claimed and executed |
| `queue` | inserted | not claimed |
| `reject` | fails before insert | not claimed |
| `drop` | returns an acknowledgement id without insert | not claimed |

To queue `send-email` rows without executing them, set the task type mode to `queue`:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: queue
```

Queued tasks remain in the database and are claimed when `mode` is set back to `execute`.

Use `mode: reject` to fail new queue attempts:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: reject
```

Use `mode: drop` to accept new queue attempts without inserting task rows:

```yaml
mango4j:
  swarm:
    task-types:
      send-email:
        mode: drop
```
