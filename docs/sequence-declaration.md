# Sequence declaration reference

Sequencing is declared in a YAML descriptor, by default loaded from
`classpath:/messaging/jeap-sequential-inbox.yml` (override with
`jeap.messaging.sequential-inbox.config-location`). The file has two top-level keys: an optional
`subTypeResolvers` map and a list of `sequences`.

## Sequence

| Attribute         | Cardinality | Description                                                                                     | Example       |
|-------------------|-------------|-------------------------------------------------------------------------------------------------|---------------|
| `name`            | Required    | Name of the sequence (used in logs, the REST API and the sequence instance rows)                | `OrderSequence` |
| `retentionPeriod` | Required    | How long a sequence instance is retained, as a `Duration`; drives expiry and housekeeping       | `24h`         |
| `messages`        | Required    | The list of `SequencedMessageType` entries that belong to the sequence                          |               |

## SequencedMessageType

| Attribute            | Cardinality | Description                                                                                                                                                                 | Example |
|----------------------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `type`               | Required    | The jEAP message type name (the Avro message simple class name)                                                                                                            | `JmeOrderCreatedEvent` |
| `subType`            | Optional    | Subtype for fine-grained sequencing of a generic message type. Resolved by a `SubTypeResolver`; must be a value of the resolver's Java `Enum`                              | `STOCK_AVAILABLE` |
| `topic`              | Optional    | Topic to consume the message from, if different from the default                                                                                                          | `my-topic` |
| `clusterName`        | Optional    | Kafka cluster of the topic, if different from the default cluster                                                                                                          | `aws` |
| `contextIdExtractor` | Required    | Fully-qualified class name implementing `ContextIdExtractor`; returns the `contextId` grouping messages into one instance (`null` = not sequenceable)                      | `ch.admin.bit.example.OrderIdExtractor` |
| `messageFilter`      | Optional    | Fully-qualified class name implementing `MessageFilter`; `shouldSequence` returning `false` means the message bypasses the inbox and is handled immediately                | `ch.admin.bit.example.OrderCreatedEventFilter` |
| `releaseCondition`   | Optional    | The predecessor(s) that must be processed before this message is released. No condition means the message can be released as soon as it arrives                            |         |

## Release conditions

A release condition references predecessors by their qualified name (`type`, or
`type.subType` when a subtype is used). Conditions can be nested with `and` and `or`.

```yaml
# single predecessor
releaseCondition:
  predecessor: JmeOrderCreatedEvent

# all of several predecessors
releaseCondition:
  and:
    - predecessor: JmeOrderValidatedEvent.STOCK_AVAILABLE
    - predecessor: JmeOrderValidatedEvent.CUSTOMER_CREDIT_CHECKED
    - predecessor: JmeOrderPreparedEvent

# any one of several predecessors
releaseCondition:
  or:
    - predecessor: JmeOrderShippedEvent
    - predecessor: JmeOrderOtherEvent
```

## subTypeResolvers

When one Avro message type represents several business events, map it to a `SubTypeResolver` so the
inbox can distinguish the subtypes. The resolver returns an `Enum`; every enum value must be declared
as a `subType` in the sequence, and every `subType` must be a valid enum value.

```yaml
subTypeResolvers:
  JmeOrderValidatedEvent: ch.admin.bit.example.OrderValidatedEventSubTypeResolver
```

## Full example

```yaml
subTypeResolvers:
  JmeOrderValidatedEvent: ch.admin.bit.example.OrderValidatedEventSubTypeResolver

sequences:
  - name: OrderSequence
    retentionPeriod: 24h
    messages:
      - type: JmeOrderCreatedEvent
        contextIdExtractor: ch.admin.bit.example.OrderIdExtractor
        messageFilter: ch.admin.bit.example.OrderCreatedEventFilter

      - type: JmeOrderValidatedEvent
        subType: STOCK_AVAILABLE
        contextIdExtractor: ch.admin.bit.example.OrderIdExtractor
        releaseCondition:
          predecessor: JmeOrderCreatedEvent

      - type: JmeOrderValidatedEvent
        subType: CUSTOMER_CREDIT_CHECKED
        contextIdExtractor: ch.admin.bit.example.OrderIdExtractor
        releaseCondition:
          predecessor: JmeOrderCreatedEvent

      - type: JmeOrderShippedEvent
        contextIdExtractor: ch.admin.bit.example.OrderIdExtractor
        releaseCondition:
          and:
            - predecessor: JmeOrderValidatedEvent.STOCK_AVAILABLE
            - predecessor: JmeOrderValidatedEvent.CUSTOMER_CREDIT_CHECKED
```

## Related

- [Getting started](getting-started.md)
- [How sequencing works](how-it-works.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-sequential-inbox](../README.md)
