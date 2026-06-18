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

## Metrics

The library registers Micrometer meters, recomputed every `metrics.update-rate-minutes` (default 5).

| Metric                                                          | Type  | Tags   | Meaning                                                                  |
|----------------------------------------------------------------|-------|--------|--------------------------------------------------------------------------|
| `jeap.messaging.sequential-inbox.waiting-messages`             | gauge | `type` | Buffered (`WAITING`) messages per sequenced message type                 |
| `jeap.messaging.sequential-inbox.waiting-message-delay`        | gauge | `type` | Current delay of waiting messages per type                               |
| `jeap.messaging.sequential-inbox.consumed-messages`            | gauge | `type` | Messages consumed by the inbox                                           |
| `jeap.messaging.sequential-inbox.expiring-soon-sequences`      | gauge | `type` | Open instances past `expiring-percentile` (default 0.75) of retention    |
| `jeap.messaging.sequential-inbox.retention-period-expired-sequences` | gauge | `type` | Instances whose retention period has elapsed                      |
| `jeap.messaging.sequential-inbox.deleted-by-housekeeping-sequences`  | gauge | `type` | Instances removed by housekeeping                                |

In addition the sequencing service exposes timers
(`jeap.messaging.sequential-inbox.handle-message` and the pending-action handlers) with the
percentiles `0.5, 0.8, 0.95, 0.99`.

## Related

- [Configuration reference](configuration.md)
- [How sequencing works](how-it-works.md)
- [DevOps operations](devops-operations.md)
- [jeap-messaging-sequential-inbox](../README.md)
