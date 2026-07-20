# Sequential Inbox Idempotence and Concurrent Message Handling

## Purpose

The Sequential Inbox must not process concurrent deliveries or store the same logical message more than once through its claim-protected path. A logical message is identified by its qualified message type and idempotence ID. The configuration assigns every qualified message type to at most one sequence type, so the sequence name is not part of the claim key.

This document describes why Kafka partition ordering alone is insufficient for this guarantee and how the Sequential Inbox uses an atomic database claim to handle concurrent duplicate deliveries.

## Background

Kafka normally assigns a partition to only one consumer within a consumer group. Records from that partition are delivered sequentially to that consumer. If duplicate messages use the same key, are published to the same topic, and the partition assignment remains consistent, they are consequently not processed concurrently.

The former Sequential Inbox idempotence check implicitly relied on that behavior:

```text
receive message
-> look for an existing sequenced_message
-> invoke or buffer the message
-> store sequenced_message
```

The check and the insert were separate operations. This is safe for sequential delivery, but not when two transactions receive the same logical message at almost the same time. Neither transaction can see an uncommitted row from the other transaction, so both may observe that no Inbox record exists and both may continue processing.

Kafka partitioning remains useful for ordering, but it is no longer the correctness boundary for Inbox idempotence. The database makes the final atomic decision.

## Required guarantee

For the claim key

```text
qualified message type + idempotence ID
```

the Sequential Inbox requires the following behavior:

1. At most one concurrent delivery may win the claim.
2. A losing delivery must not invoke the listener or create another Inbox record.
3. If the winner commits, later duplicates must be skipped.
4. If the winner rolls back, another delivery must be able to take over.
5. A failed buffered message must remain retryable.
6. Existing databases must be upgradeable without mandatory cleanup of historical duplicates.

This is a processing and storage guarantee for messages that enter the sequencing path. A message filtered out of sequencing because its context ID extractor returns `null` is handled directly and does not use this claim.

## Solution overview

The implementation uses a transactional insert-first claim:

```text
transactional insert-first claim
+ existing sequenced_message lookup
```

The claim serializes concurrent processing before the listener or buffering logic is reached. The existing non-unique index on `sequenced_message.idempotence_id` remains unchanged. In particular, the migration does not add a unique constraint that would require consumers to clean up historical duplicate data before upgrading.

## Claim table

The claim table needs to be created by the migration:

```sql
CREATE TABLE sequential_inbox_idempotence
(
    message_type         text                     not null,
    idempotence_id       text                     not null,
    sequence_instance_id bigint                   not null references sequence_instance ON DELETE CASCADE,
    created_at           timestamp with time zone NOT NULL,
    CONSTRAINT sequential_inbox_idempotence_pkey PRIMARY KEY (message_type, idempotence_id)
);

CREATE INDEX sequential_inbox_idempotence_sequence_instance_id
    ON sequential_inbox_idempotence (sequence_instance_id);
```

The composite primary key represents the existing service-wide idempotence scope. The Sequential Inbox configuration requires every qualified message type to belong to at most one sequence type, and the existing Inbox lookup also uses `message_type + idempotence_id`. The claim therefore uses the same key and does not store the redundant sequence name. The primary key gives PostgreSQL the unique constraint needed to arbitrate concurrent inserts.

`sequence_instance_id` connects the claim to Inbox retention. When housekeeping deletes the sequence instance, PostgreSQL removes its claims through `ON DELETE CASCADE`.

## Atomic claim operation

The claim is the first database operation inside the processing transaction:

```sql
INSERT INTO sequential_inbox_idempotence
    (message_type, idempotence_id, sequence_instance_id, created_at)
VALUES (?, ?, ?, NOW())
ON CONFLICT (message_type, idempotence_id) DO NOTHING;
```

The affected-row count determines the outcome:

- `1`: the claim was inserted; this transaction is the winner and may continue;
- `0`: the claim already exists or a concurrent transaction committed it; this delivery is skipped.

`ON CONFLICT (message_type, idempotence_id) DO NOTHING` is a PostgreSQL feature. The explicit conflict target ensures that only a conflict on the claim identity is treated as an already claimed message; conflicts on any future unique constraint remain errors. The correctness of the claim comes from the primary key and transaction semantics, not from an application-level check.

## Concurrent delivery

Two transactions can issue the insert at almost the same time. They cannot both commit the same primary key.

### Winner commits

```text
Instance A / transaction A              Instance B / transaction B
────────────────────────────────────────────────────────────────────
INSERT claim
-> returns 1
-> claim is not committed yet

continue Inbox processing               INSERT same claim
invoke or buffer message                 -> waits inside PostgreSQL
store Inbox state

COMMIT transaction A
claim becomes visible
                                         wait ends
                                         conflict is confirmed
                                         insert returns 0
                                         skip listener and Inbox insert
                                         COMMIT transaction B
```

The losing thread waits in the database. It does not pass the claim and does not execute the listener while the winner is in flight.

### Winner rolls back

```text
Instance A / transaction A              Instance B / transaction B
────────────────────────────────────────────────────────────────────
INSERT claim -> returns 1                INSERT same claim -> waits

processing fails
ROLLBACK transaction A
claim insert is rolled back
                                         wait ends
                                         insert succeeds -> returns 1
                                         B takes over processing
```

No lease or stale-claim cleanup is needed for an uncommitted claim. PostgreSQL rolls it back when the transaction rolls back or its connection is lost.

## Transaction boundaries and guarantee

The claim transaction stays open for the complete Inbox decision, but listener and Inbox-state commits remain separate:

```text
T1 claim transaction
├─ INSERT claim
├─ suspend T1; invoke listener with NOT_SUPPORTED
│  └─ optional listener-owned business transaction
├─ suspend T1; store sequenced_message in REQUIRES_NEW
└─ COMMIT T1
```

The claim prevents concurrent listener execution. It does not provide exactly-once business effects if the listener commits and the service terminates before the Inbox state is stored. Listeners with non-idempotent effects must provide their own idempotency or use an outbox pattern.

## Failures and retries

An immediate processing failure rolls back the uncommitted claim, allowing a resend or waiting delivery to take over. A buffered message already has a committed claim; if its later processing fails, the Inbox atomically writes `FAILED` and deletes that claim in one `REQUIRES_NEW` transaction. A resend can then claim and retry it.

If a concurrent winner commits successfully, the loser is an already-handled duplicate and is acknowledged without an error. If the winner rolls back, PostgreSQL lets the waiting loser insert the claim and become the new processing winner.

## Migration notes

The migration only creates the claim table and keeps the existing non-unique `sequenced_message` index. Historical duplicates therefore do not block deployment, but they are not repaired: replaying an affected idempotence ID can still make the single-result Inbox lookup fail. Avoid prolonged mixed-version consumer operation because older instances do not acquire claims.

The claim transaction also holds a database connection while the listener runs. Listener duration, database timeouts, connection-pool capacity, and Kafka `max.poll.interval.ms` must be configured accordingly.

## Retention boundary

Claims are deleted with their sequence instance. Consequently, the idempotence guarantee has the same retention boundary as the sequence. A duplicate arriving after housekeeping removed the complete sequence history can be processed as a new message.
