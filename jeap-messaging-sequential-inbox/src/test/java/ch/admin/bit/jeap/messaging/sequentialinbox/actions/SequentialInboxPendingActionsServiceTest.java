package ch.admin.bit.jeap.messaging.sequentialinbox.actions;

import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.SequentialInboxService;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SequentialInboxPendingActionsServiceTest {

    @Mock
    private MessageRepository messageRepository;
    @Mock
    private SequenceInstanceRepository sequenceInstanceRepository;
    @Mock
    private SequentialInboxService sequentialInboxService;

    @InjectMocks
    private SequentialInboxPendingActionsService service;

    @Test
    void runPendingActionsOnMessages_shouldProcessMessages() {
        SequencedMessage message1 = mock(SequencedMessage.class);
        SequencedMessage message2 = mock(SequencedMessage.class);
        when(messageRepository.getMessagesWithPendingAction()).thenReturn(List.of(message1, message2));

        service.runPendingActionsOnMessages();

        verify(sequentialInboxService).handleMessageWithPendingAction(message1);
        verify(sequentialInboxService).handleMessageWithPendingAction(message2);
    }

    @Test
    void runPendingActionsOnMessages_shouldNotProcessWhenEmpty() {
        when(messageRepository.getMessagesWithPendingAction()).thenReturn(List.of());

        service.runPendingActionsOnMessages();

        verify(sequentialInboxService, never()).handleMessageWithPendingAction(any());
    }

    @Test
    void runPendingActionsOnSequences_shouldProcessSequences() {
        SequenceInstance seq1 = mock(SequenceInstance.class);
        SequenceInstance seq2 = mock(SequenceInstance.class);
        when(sequenceInstanceRepository.findAllByPendingActionIsNotNull()).thenReturn(List.of(seq1, seq2));

        service.runPendingActionsOnSequences();

        verify(sequentialInboxService).handleSequenceWithPendingAction(seq1);
        verify(sequentialInboxService).handleSequenceWithPendingAction(seq2);
    }

    @Test
    void runPendingActionsOnSequences_shouldNotProcessWhenEmpty() {
        when(sequenceInstanceRepository.findAllByPendingActionIsNotNull()).thenReturn(List.of());

        service.runPendingActionsOnSequences();

        verify(sequentialInboxService, never()).handleSequenceWithPendingAction(any());
    }
}
