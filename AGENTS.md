# AGENTS.md

Guidance for AI coding agents working **in this repository**. For how to *use* the library in a
consuming service, read [README.md](README.md) and the [docs/](docs/) folder instead.

## Project

jEAP Messaging Sequential Inbox is a multi-module Maven library that lets a Spring Boot microservice
process Kafka messages in a declared order rather than in arrival order. Messages that share a
`contextId` form a sequence instance; a message is released to the application only once its
configured release conditions (predecessor messages) are met, otherwise it is buffered in the
database and replayed later. It builds on `jeap-messaging` and reuses its Avro model
(`AvroMessage`/`AvroMessageKey`), at-least-once error handling and idempotence.

## Repository layout

```
pom.xml                                        # Parent POM (packaging=pom); declares the modules below
jeap-messaging-sequential-inbox/               # The library
  .../configuration/model/                     # SequentialInboxConfiguration, Sequence, SequencedMessageType,
                                               #   ReleaseCondition, ContextIdExtractor, MessageFilter, SubTypeResolver
  .../configuration/deserializer/              # YAML loader + Jackson deserializers for the descriptor
  .../inbox/                                   # SequentialInboxService and the sequencing/buffering services
  .../spring/                                  # @AutoConfiguration, @SequentialInboxMessageListener, handler wiring
  .../kafka/                                   # Kafka consumer factory + listener that feed the inbox
  .../persistence/ + .../jpa/                  # JPA entities (SequenceInstance, SequencedMessage, BufferedMessage) + repos
  .../housekeeping/                            # Retention/expiry/removal scheduled jobs
  .../actions/                                 # Pending-action processing (CONSUME, EXPIRE, CONSUME_ALL, CLOSE)
  .../metrics/                                 # Micrometer gauges/timers
jeap-messaging-sequential-inbox-rest-api/      # Optional role-protected REST API (read + pending actions)
jeap-messaging-sequential-inbox-test/          # Integration-test harness (EmbeddedKafka / Testcontainers); no main sources
Jenkinsfile, publiccode.yml, CHANGELOG.md, LICENSE
```

## Build & test

```bash
./mvnw -pl jeap-messaging-sequential-inbox -am install   # build the library and its dependencies
./mvnw verify                                            # full build incl. integration tests
```

- Parent: `ch.admin.bit.jeap:jeap-internal-spring-boot-parent` (Spring Boot 4 aligned).
- Integration tests live in `jeap-messaging-sequential-inbox-test` and use EmbeddedKafka and a
  Postgres Testcontainer; the test descriptors are under `src/test/resources/messaging/*.yml`.
- Spring Boot 3 maintenance happens on the `release/springboot3` branch.

## jEAP conventions

- Java packages live under `ch.admin.bit.jeap.messaging.sequentialinbox...`.
- Configuration properties use the prefix `jeap.messaging.sequential-inbox.*`.
- Auto-configuration is registered via `@AutoConfiguration` and
  `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` (the library
  registers four auto-configurations; the REST API module registers one more).
- The sequencing descriptor is loaded from `classpath:/messaging/jeap-sequential-inbox.yml` by
  default; override with `jeap.messaging.sequential-inbox.config-location`.
- The library does **not** create its database tables; the consuming service must add the Flyway
  migration (see `docs/getting-started.md`).
- All Kafka keys/values are Avro objects extending the base types from `jeap-messaging-avro`.
- Application code receives released messages through methods annotated `@SequentialInboxMessageListener`
  (one parameter `AvroMessage`, or two parameters `AvroMessageKey, AvroMessage`).

## Docs

When changing public behaviour, update the matching focused file under [docs/](docs/) (one topic per
file) and the documentation index in the README. Ground every documented fact in the source.

## Versioning

- Semantic Versioning; all changes documented in [CHANGELOG.md](./CHANGELOG.md) (Keep a Changelog format).
- `setPomVersions.sh <version>` updates the version across all module POMs.
- On a feature branch use `x.y.z-SNAPSHOT`; always keep the `-SNAPSHOT` postfix in the POMs (CI
  removes it on release). Do not use the SNAPSHOT postfix in CHANGELOG or `publiccode.yml`.
- Use the JIRA ID from the branch name as the commit-message prefix (e.g. `JEAP-1234 Added feature X`);
  do not use conventional commits.
- When bumping the version, also update the changelog and the version/date in `publiccode.yml`.
