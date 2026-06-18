# DevOps operations

The Sequential Inbox lets operators intervene on stuck messages and sequence instances. Write
operations are **asynchronous**: they set a `pending_action` in the database, and a scheduled job
processes it in batches. The default scheduling intervals and batch sizes are configurable (see the
pending-actions properties in the [Configuration reference](configuration.md)).

## Supported actions

### Message actions

Set on the `sequenced_message.pending_action` column (enum `SequencedMessagePendingAction`):

| Action    | Description                                                       |
|-----------|------------------------------------------------------------------|
| `CONSUME` | Process the message now, regardless of its current state         |
| `EXPIRE`  | Mark the message as consumed without running the handler         |

### Sequence instance actions

Set on the `sequence_instance.pending_action` column (enum `SequenceInstancePendingAction`):

| Action        | Description                                                         |
|---------------|---------------------------------------------------------------------|
| `CONSUME_ALL` | Process all currently buffered messages of the instance immediately |
| `CLOSE`       | Close the instance without processing remaining buffered messages   |

A pending action can be written directly via SQL, or via the REST API below.

## REST API (optional module)

The REST API is **not** enabled by default. Add the `jeap-messaging-sequential-inbox-rest-api`
dependency to expose it. Endpoints are under `/api` and protected with jEAP semantic roles:

- Read operations require the `sequentialinbox` `view` role.
- Write operations require the `sequentialinbox` `write` role.

| Method & path                                          | Role  | Description                                                                 |
|--------------------------------------------------------|-------|-----------------------------------------------------------------------------|
| `GET /api/sequences?queryType=EXPIRED`                 | view  | List fully expired sequence instances (paged: `pageNumber`, `pageSize`)     |
| `GET /api/sequences?queryType=EXPIRING`                | view  | List instances past 75% of their retention period                          |
| `GET /api/sequences/{sequenceName}/{contextId}`        | view  | Get an instance with its sequenced and buffered messages                   |
| `PUT /api/sequences/{sequenceInstanceId}/pending-action?pendingAction=CLOSE\|CONSUME_ALL` | write | Schedule a pending action on a sequence instance |
| `PUT /api/messages/{sequencedMessageId}/pending-action?pendingAction=CONSUME\|EXPIRE`     | write | Schedule a pending action on a message            |

Write endpoints only set the pending action; the scheduler performs the work on its next run.

## Related

- [Configuration reference](configuration.md)
- [How sequencing works](how-it-works.md)
- [Housekeeping, retention & metrics](housekeeping-and-metrics.md)
- [jeap-messaging-sequential-inbox](../README.md)
