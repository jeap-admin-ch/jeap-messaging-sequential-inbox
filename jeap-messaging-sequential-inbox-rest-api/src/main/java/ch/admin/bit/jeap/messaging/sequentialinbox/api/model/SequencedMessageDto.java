package ch.admin.bit.jeap.messaging.sequentialinbox.api.model;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessagePendingAction;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessageState;
import com.fasterxml.jackson.annotation.JsonRawValue;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
@AllArgsConstructor
public class SequencedMessageDto {

    private Long id;

    private String messageType;

    private UUID sequencedMessageId;

    private String idempotenceId;

    private SequencedMessageState state;

    private ZonedDateTime createdAt;

    private ZonedDateTime stateChangedAt;

    private SequencedMessagePendingAction pendingAction;

    private String key;

    @JsonRawValue
    private String value;

    public static SequencedMessageDto fromSequenceMessage(SequencedMessage sequencedMessage, String key, String value) {
        return new SequencedMessageDto(
                sequencedMessage.getId(),
                sequencedMessage.getMessageType(),
                sequencedMessage.getSequencedMessageId(),
                sequencedMessage.getIdempotenceId(),
                sequencedMessage.getState(),
                sequencedMessage.getCreatedAt(),
                sequencedMessage.getStateChangedAt(),
                sequencedMessage.getPendingAction(),
                key,
                value
        );
    }

}
