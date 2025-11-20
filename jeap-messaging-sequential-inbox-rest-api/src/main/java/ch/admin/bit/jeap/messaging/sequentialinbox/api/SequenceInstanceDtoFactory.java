package ch.admin.bit.jeap.messaging.sequentialinbox.api;

import ch.admin.bit.jeap.messaging.sequentialinbox.api.model.SequenceInstanceDto;
import ch.admin.bit.jeap.messaging.sequentialinbox.api.model.SequencedMessageDto;
import ch.admin.bit.jeap.messaging.sequentialinbox.kafka.DomainEventDeserializer;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class SequenceInstanceDtoFactory {

    private final DomainEventDeserializer domainEventDeserializer;

    public SequenceInstanceDto fromSequenceInstance(SequenceInstance sequenceInstance, List<SequencedMessage> messages, List<BufferedMessage> bufferedMessages) {

        Map<Long, BufferedMessage> bufferedMessagesBySequencedMessageId = bufferedMessages.stream()
                .collect(Collectors.toMap(BufferedMessage::getSequencedMessageId, Function.identity()));

        return new SequenceInstanceDto(
                sequenceInstance.getId(),
                sequenceInstance.getName(),
                sequenceInstance.getContextId(),
                sequenceInstance.getState(),
                sequenceInstance.getCreatedAt(),
                sequenceInstance.getClosedAt(),
                sequenceInstance.getRetainUntil(),
                sequenceInstance.getRemoveAfter(),
                sequenceInstance.getPendingAction(),
                messages.stream().map(m -> SequencedMessageDto.fromSequenceMessage(m,
                        getMessageKey(m, bufferedMessagesBySequencedMessageId.get(m.getId())),
                        getMessageValue(m, bufferedMessagesBySequencedMessageId.get(m.getId())))).toList());
    }

    private String getMessageValue(SequencedMessage sequencedMessage, BufferedMessage bufferedMessage) {
        if (bufferedMessage != null && bufferedMessage.getValue() != null) {
            try {
                return domainEventDeserializer.toJsonString(sequencedMessage.getClusterName(), sequencedMessage.getTopic(), bufferedMessage.getValue());
            } catch (IOException e) {
                throw new IllegalStateException("Cannot deserialize message value", e);
            }
        }
        return null;
    }

    private String getMessageKey(SequencedMessage sequencedMessage, BufferedMessage bufferedMessage) {
        if (bufferedMessage != null && bufferedMessage.getKey() != null) {
            try {
                return domainEventDeserializer.toJsonString(sequencedMessage.getClusterName(), sequencedMessage.getTopic(), bufferedMessage.getKey());
            } catch (IOException e) {
                throw new IllegalStateException("Cannot deserialize message key", e);
            }
        }
        return null;
    }

}
