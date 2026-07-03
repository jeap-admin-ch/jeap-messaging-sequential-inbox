# Housekeeping, retention & metrics

## Retention and the sequence lifecycle

Every sequence declares a `retentionPeriod`. When a sequence instance is created, its `retain until`
timestamp is set accordingly. A sequence instance is *closed* once all its message types have been
processed. Housekeeping then deals with instances based on their retention:

- **Closed instances** are cleaned up by the `closed-instances-cron` job.
- **Expired instances** (still open after `retain until`) are marked by the `expiry-cron` job. The
  `delay` property adds a buffer on top of `retain until` so operators have time to react before the
  remaining buffered messages are forwarded to the `jeap-messaging` error-handling service.
- **Instances flagged for removal** are deleted in batches by the `delete-for-removal-cron` job
  (`sequence-removal-batch-size` per batch).

A single housekeeping run is bounded by `max-continuous-house-keeping-duration` (max 15 minutes); if
it cannot finish, the next scheduled run continues the work, possibly on another service instance.
The jobs use ShedLock so only one instance runs them at a time. See
[Configuration reference](configuration.md) for all housekeeping properties.

> **Before version 10.0.0:** sequences and their messages were deleted immediately when the retention
> period expired. Messages were not forwarded to the error-handling service. The `delay`-based
> deferred removal and forwarding behaviour described above was introduced in version 10.0.0.

## Metrics

The library registers Micrometer meters, recomputed every `metrics.update-rate-minutes` (default 5).

| Metric                                                                          | Type    | Tags     | Meaning                                                                  |
|---------------------------------------------------------------------------------|---------|----------|--------------------------------------------------------------------------|
| `jeap.messaging.sequential-inbox.waiting-messages`                              | gauge   | `type`   | Buffered (`WAITING`) messages per sequenced message type                 |
| `jeap.messaging.sequential-inbox.waiting-message-delay`                         | timer   | `type`   | Wait duration recorded when a waiting message is released                |
| `jeap.messaging.sequential-inbox.consumed-messages`                             | counter | `type`   | Messages consumed by the inbox                                           |
| `jeap.messaging.sequential-inbox.expiring-soon-sequences`                       | gauge   | `type`   | Open instances past `expiring-percentile` (default 0.75) of retention    |
| `jeap.messaging.sequential-inbox.retention-period-expired-sequences`            | gauge   | `type`   | Instances whose retention period has elapsed                             |
| `jeap.messaging.sequential-inbox.deleted-by-housekeeping-sequences`             | counter | `type`   | Instances removed by housekeeping                                        |
| `jeap.messaging.sequential-inbox.handle-message`                                | timer   | —        | Time to process an incoming message through the inbox                    |
| `jeap.messaging.sequential-inbox.handle-message-with-pending-action`            | timer   | —        | Time to execute a pending action on a sequenced message                  |
| `jeap.messaging.sequential-inbox.handle-sequence-with-pending-action`           | timer   | —        | Time to execute a pending action on a sequence instance                  |
| `jeap.messaging.sequential-inbox.housekeeping.closed`                           | timer   | —        | Time for the housekeeping run that removes closed sequence instances     |
| `jeap.messaging.sequential-inbox.housekeeping.expired`                          | timer   | —        | Time for the housekeeping run that marks expired sequence instances      |
| `jeap.messaging.sequential-inbox.housekeeping.delete-for-removal`               | timer   | —        | Time for the housekeeping run that deletes instances flagged for removal |

All timers are recorded with percentiles `0.5, 0.8, 0.95, 0.99`.

## Related

- [Configuration reference](configuration.md)
- [How sequencing works](how-it-works.md)
- [DevOps operations](devops-operations.md)
- [jeap-messaging-sequential-inbox](../README.md)
