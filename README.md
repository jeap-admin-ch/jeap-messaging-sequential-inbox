# jEAP Messaging Sequential Inbox

The jEAP Sequential Inbox lets a microservice control the order in which Kafka messages are
processed, even when that order differs from the order in which the messages arrive. Sequencing is
declared by the DevOps team in a YAML descriptor: messages sharing the same `contextId` form a
*sequence instance*, and a message is only handed to the application once its configured release
conditions (its predecessor messages) are satisfied. Messages that are not yet releasable are
buffered in the database and replayed later. The library builds on
[jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging) and complements its at-least-once,
idempotent delivery.

* Declarative, YAML-based sequencing of Kafka messages by `contextId`, message type and optional subtype
* Release conditions with `predecessor`, `and` and `or` operators to express message dependencies
* Buffering of not-yet-releasable messages in a JPA-backed store, with idempotent handling
* Per-message `contextIdExtractor`, optional `messageFilter` (bypass sequencing) and `subTypeResolver`
* Retention, housekeeping and metrics for sequence instances and buffered messages
* DevOps operations (force-consume, expire, close) via pending actions and an optional REST API
* Multi-cluster Kafka consumption and recording-mode migration for introducing a sequence on a live topic

## Documentation

Start with [Getting started](docs/getting-started.md), then follow the links below.

| Topic                                                        | File                                                                       |
|--------------------------------------------------------------|----------------------------------------------------------------------------|
| Getting started (add the dependency, declare a sequence)     | [docs/getting-started.md](docs/getting-started.md)                         |
| How sequencing works (buffering & release flow)              | [docs/how-it-works.md](docs/how-it-works.md)                               |
| Sequence declaration reference (`jeap-sequential-inbox.yml`) | [docs/sequence-declaration.md](docs/sequence-declaration.md)               |
| Configuration reference (`jeap.messaging.sequential-inbox.*`)| [docs/configuration.md](docs/configuration.md)                             |
| Housekeeping, retention & metrics                            | [docs/housekeeping-and-metrics.md](docs/housekeeping-and-metrics.md)       |
| DevOps operations (pending actions & REST API)               | [docs/devops-operations.md](docs/devops-operations.md)                     |

## Modules

Group id for all modules is `ch.admin.bit.jeap`; the version is managed by the jEAP Spring Boot
parent. The artifact a consuming service depends on is `jeap-messaging-sequential-inbox`.

| Module                                       | Purpose                                                                                                    |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `jeap-messaging-sequential-inbox`            | The library: auto-configuration, sequencing service, JPA persistence, Kafka listeners, housekeeping, metrics |
| `jeap-messaging-sequential-inbox-rest-api`   | Optional REST API exposing read operations and DevOps pending actions (role-protected)                    |
| `jeap-messaging-sequential-inbox-test`       | Internal integration-test harness for the library (EmbeddedKafka / Testcontainers); not a consumer artifact |

## Changes

This library is versioned using [Semantic Versioning](http://semver.org/) and all changes are documented in
[CHANGELOG.md](./CHANGELOG.md) following the format defined in [Keep a Changelog](http://keepachangelog.com/).

## Note

This repository is part the open source distribution of jEAP. See [github.com/jeap-admin-ch/jeap](https://github.com/jeap-admin-ch/jeap)
for more information.

## License

This repository is Open Source Software licensed under the [Apache License 2.0](./LICENSE).
