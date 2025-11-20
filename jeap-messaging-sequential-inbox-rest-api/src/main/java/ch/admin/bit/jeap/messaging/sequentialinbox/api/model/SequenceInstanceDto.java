package ch.admin.bit.jeap.messaging.sequentialinbox.api.model;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstancePendingAction;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
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

}
