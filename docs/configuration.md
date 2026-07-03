# Configuration reference

All properties use the prefix `jeap.messaging.sequential-inbox`. Sequencing behaviour itself is
declared in the YAML descriptor (see [Sequence declaration reference](sequence-declaration.md)); the
properties below tune the runtime, housekeeping, pending-action and metrics jobs.

## Kafka consumer configuration

When a message is released, the inbox may immediately release further buffered messages for the same
`contextId` within the same Kafka poll. This means a single Kafka poll can trigger the processing of
several messages. If technical processing tends to be on the longer side, the default
`max.poll.interval.ms` may not be sufficient. Increase it to account for the maximum time needed to
process one received message plus all messages that may be released as a result.

See the [Kafka Consumer configuration](https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms)
for details.

## Hikari connection-pool configuration

The Sequential Inbox holds a database lock for the duration of each message-processing call (see
[Multi-instance support](how-it-works.md#multi-instance-support)). The default Hikari maximum pool
size of `4` is too small and can cause connection starvation under load. Set it to at least `10`:

```yaml
jeap:
  datasource:
    hikari:
      maximum-pool-size: 10
```

## Core

| Name                                        | Default                                          | Description                                                                                                                                                         |
|---------------------------------------------|--------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enabled`                                   | `true`                                           | Enable the Sequential Inbox auto-configuration                                                                                                                      |
| `config-location`                           | `classpath:/messaging/jeap-sequential-inbox.yml` | Location of the sequencing descriptor                                                                                                                               |
| `sequencing-start-timestamp`                | —                                                | When set, enables recording mode until this `LocalDateTime`: predecessors are processed immediately and only recorded (see [How sequencing works](how-it-works.md)) |
| `idle-lock-timeout-seconds`                 | `600`                                            | Sets `idle_in_transaction_session_timeout` (as `value * 1000` ms) for the sequence-instance lock transaction. Set to `-1` to use the database server default.       |

## Housekeeping (`jeap.messaging.sequential-inbox.housekeeping.*`)

| Name                                   | Default                | Description                                                                                                        |
|----------------------------------------|------------------------|--------------------------------------------------------------------------------------------------------------------|
| `enabled`                              | `true`                 | Enable the housekeeping jobs                                                                                       |
| `delay`                                | — (required)           | Buffer `Duration` added to a sequence's `retain until` before housekeeping forwards its messages to error handling |
| `max-continuous-house-keeping-duration`| `15m`                  | Max time a single housekeeping run may run continuously; cannot exceed 15 minutes                                  |
| `sequence-removal-batch-size`          | `10`                   | Number of sequence instances deleted per batch                                                                     |
| `closed-instances-cron`                | `0 0/15 * * * *`       | Cron for cleaning up closed sequence instances                                                                     |
| `expiry-cron`                          | `0 5/15 * * * *`       | Cron for marking expired sequence instances                                                                        |
| `delete-for-removal-cron`              | `0 10/15 * * * *`      | Cron for removing sequence instances flagged for removal                                                           |

## Pending actions (`jeap.messaging.sequential-inbox.pending-actions.*`)

These control the scheduler that processes DevOps pending actions (see [DevOps operations](devops-operations.md)).

| Name             | Default            | Description                                             |
|------------------|--------------------|---------------------------------------------------------|
| `messages-cron`  | `0 0/2 * * * *`    | Cron checking for pending actions on messages           |
| `sequences-cron` | `0 1/2 * * * *`    | Cron checking for pending actions on sequence instances |
| `lock-at-least`  | `5s`               | Minimum ShedLock hold time for the job                  |
| `lock-at-most`   | `30m`              | Maximum ShedLock hold time for the job                  |
| `page-size`      | `50`               | Query page size                                         |
| `max-pages`      | `10`               | Maximum pages processed per run (bounds the run time)   |

## Metrics (`jeap.messaging.sequential-inbox.metrics.*`)

| Name                  | Default | Description                                                                       |
|-----------------------|---------|-----------------------------------------------------------------------------------|
| `update-rate-minutes` | `5`     | How often the gauge metrics are recomputed                                        |
| `expiring-percentile` | `0.75`  | Fraction of the retention period after which a sequence counts as "expiring soon" |

See [Housekeeping, retention & metrics](housekeeping-and-metrics.md) for the exported metric names.

## Related

- [Getting started](getting-started.md)
- [Sequence declaration reference](sequence-declaration.md)
- [Housekeeping, retention & metrics](housekeeping-and-metrics.md)
- [DevOps operations](devops-operations.md)
- [jeap-messaging-sequential-inbox](../README.md)
