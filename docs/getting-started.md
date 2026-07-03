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
the tables itself. Add the schema as a Flyway migration in your service. The canonical DDL is in
[`jeap-messaging-sequential-inbox-test/src/test/resources/db/migration/V1__create-sequential-inbox-schema.sql`](../jeap-messaging-sequential-inbox-test/src/test/resources/db/migration/V1__create-sequential-inbox-schema.sql):

```sql
CREATE SEQUENCE sequence_instance_sequence START WITH 1 INCREMENT 1 CYCLE; -- sequence is not used via Hibernate, not caching 50 values
CREATE SEQUENCE buffered_message_sequence START WITH 1 INCREMENT 50 CYCLE;
CREATE SEQUENCE sequenced_message_sequence START WITH 1 INCREMENT 50 CYCLE;

CREATE TABLE sequence_instance
(
    id           bigint                   not null
        constraint sequence_instance_pkey primary key,
    name         text                     not null,
    context_id   text                     not null,
    state        text                     not null,
    created_at   timestamp with time zone NOT NULL,
    closed_at    timestamp with time zone,
    retain_until timestamp with time zone NOT NULL
);

ALTER TABLE sequence_instance
    ADD CONSTRAINT SEQUENCE_INSTANCE_NAME_CONTEXT_ID_UK UNIQUE (name, context_id);

CREATE TABLE sequenced_message
(
    id                   bigint                   not null
        constraint sequenced_message_pkey primary key,
    message_type         text                     not null,
    cluster_name         text                     not null,
    topic                text                     not null,
    sequenced_message_id UUID                     not null,
    idempotence_id       text                     not null,
    state                text                     not null,
    trace_id_high        bigint,
    trace_id             bigint,
    span_id              bigint,
    parent_span_id       bigint,
    trace_id_string      text,
    created_at           timestamp with time zone NOT NULL,
    state_changed_at     timestamp with time zone,
    sequence_instance_id bigint references sequence_instance
);

CREATE INDEX sequenced_message_sequence_instance_id ON sequenced_message (sequence_instance_id);
CREATE INDEX sequenced_message_idempotence_id ON sequenced_message (idempotence_id);
CREATE INDEX idx_sequenced_message_state_message_type ON sequenced_message (state, message_type); -- for metrics

CREATE TABLE buffered_message
(
    id                   bigint not null
        constraint buffered_message_pkey primary key,
    sequence_instance_id bigint references sequence_instance,
    sequenced_message_id bigint references sequenced_message,
    message_key          bytea,
    message_value        bytea  not null
);

CREATE INDEX buffered_message_sequence_instance_id ON buffered_message (sequence_instance_id);
CREATE INDEX buffered_message_sequenced_message_id ON buffered_message (sequenced_message_id);

CREATE SEQUENCE message_header_sequence START WITH 1 INCREMENT 50 CYCLE;

CREATE TABLE message_header
(
    id                  bigint not null
        constraint message_header_pkey primary key,
    buffered_message_id bigint not null references buffered_message,
    header_name         text   not null,
    header_value        bytea  not null
);

CREATE INDEX message_header_buffered_message_id ON message_header (buffered_message_id);

CREATE TABLE shedlock
(
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMP    NOT NULL,
    locked_at  TIMESTAMP    NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);
```

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
idempotent; the inbox additionally de-duplicates on the message idempotence id. The exception is
when a message is in state `FAILED` and a retry arrives from the Error Handling Service — in that
case the same event may be delivered again.

As with all jEAP message consumers, declare a `@JeapMessageConsumerContract` for each consumed
message type on your Spring Boot application class:

```java
@SpringBootApplication
@EnableJpaRepositories
@JeapMessageConsumerContract(JmeOrderCreatedEvent.TypeRef.class)
@JeapMessageConsumerContract(JmeOrderShippedEvent.TypeRef.class)
@JeapMessageConsumerContract(JmeOrderValidatedEvent.TypeRef.class)
@JeapMessageConsumerContract(JmeOrderPreparedEvent.TypeRef.class)
public class SequentialInboxApplication {
    public static void main(String[] args) {
        SpringApplication.run(SequentialInboxApplication.class, args);
    }
}
```

## Related

- [How sequencing works](how-it-works.md)
- [Sequence declaration reference](sequence-declaration.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-sequential-inbox](../README.md)
