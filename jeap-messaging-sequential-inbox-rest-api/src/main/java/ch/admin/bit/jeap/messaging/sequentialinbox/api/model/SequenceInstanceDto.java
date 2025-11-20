package ch.admin.bit.jeap.messaging.sequentialinbox.api.model;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstancePendingAction;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.List;

@Data
@AllArgsConstructor
public class SequenceInstanceDto {

    private Long id;

    private String name;

    private String contextId;

    private SequenceInstanceState state;

    private ZonedDateTime createdAt;

    private ZonedDateTime closedAt;

    private ZonedDateTime retainUntil;

    private ZonedDateTime removeAfter;

    private SequenceInstancePendingAction pendingAction;

    private List<SequencedMessageDto> messages;

    public static SequenceInstanceDto fromSequenceInstance(SequenceInstance sequenceInstance, List<SequencedMessage> messages) {
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
                messages.stream().map(SequencedMessageDto::fromSequenceMessage).toList());
    }

}
