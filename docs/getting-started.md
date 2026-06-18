# Getting started

This page shows how to add the Sequential Inbox to a Spring Boot service that already uses
[jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging), declare a sequence, and process
the released messages. For the bigger picture see [How sequencing works](how-it-works.md).

## 1. Add the dependency

```xml
<dependency>
    <groupId>ch.admin.bit.jeap</groupId>
    <artifactId>jeap-messaging-sequential-inbox</artifactId>
</dependency>
```

The version is managed by the jEAP Spring Boot parent. Add `jeap-messaging-sequential-inbox-rest-api`
as well if you want the [DevOps REST API](devops-operations.md).

## 2. Add the database schema

The library stores sequence instances and buffered messages in the database but does **not** create
the tables itself. Add the schema as a Flyway migration in your service. The canonical DDL lives in
the repository at
`jeap-messaging-sequential-inbox-test/src/test/resources/db/migration/V1__create-sequential-inbox-schema.sql`
and creates `sequence_instance`, `sequenced_message`, `buffered_message`, `message_header` and a
`shedlock` table.

If a `shedlock` table already exists (e.g. because `@IdempotentMessageHandler` is also used), do not
add it a second time.

## 3. Declare the sequences

Create `src/main/resources/messaging/jeap-sequential-inbox.yml`. Each sequence lists the message
types it sequences, how to extract the `contextId`, and the release condition (predecessors) for each
message. See the full [Sequence declaration reference](sequence-declaration.md).

```yaml
sequences:
  - name: OrderSequence
    retentionPeriod: 24h
    messages:
      - type: JmeOrderCreatedEvent
        contextIdExtractor: ch.admin.bit.example.OrderIdExtractor

      - type: JmeOrderShippedEvent
        contextIdExtractor: ch.admin.bit.example.OrderIdExtractor
        releaseCondition:
          predecessor: JmeOrderCreatedEvent
```

The default location can be changed with `jeap.messaging.sequential-inbox.config-location`.

## 4. Implement a `ContextIdExtractor`

Each sequenced message type needs a `ContextIdExtractor` that returns the id grouping messages into
the same sequence instance (for example the order id). Returning `null` means the message cannot be
sequenced.

```java
public class OrderIdExtractor implements ContextIdExtractor<AvroMessage> {
    @Override
    public String extractContextId(AvroMessage message) {
        return message.getOptionalProcessId().orElse(null);
    }
}
```

## 5. Handle released messages

Annotate handler methods with `@SequentialInboxMessageListener`. The library starts the Kafka
consumers and invokes your method only once a message is released by its release condition. The
method takes either the Avro message, or the key and the message.

```java
@Component
class OrderListener {

    @SequentialInboxMessageListener
    void onOrderShipped(JmeOrderShippedEvent event) {
        // ... processing in the declared order ...
    }

    @SequentialInboxMessageListener
    void onOrderCreated(AvroMessageKey key, JmeOrderCreatedEvent event) {
        // two-parameter variant when the key is needed
    }
}
```

Do not add a `@KafkaListener` for sequenced topics yourself — the inbox owns the consumers and
buffers messages that are not yet releasable. Delivery is still at-least-once, so handlers must be
idempotent; the inbox additionally de-duplicates on the message idempotence id.

## Related

- [How sequencing works](how-it-works.md)
- [Sequence declaration reference](sequence-declaration.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-sequential-inbox](../README.md)
