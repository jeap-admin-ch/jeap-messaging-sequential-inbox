package ch.admin.bit.jeap.messaging.sequentialinbox.actions;

import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.SequentialInboxService;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.transaction.PlatformTransactionManager;

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
    @Mock
    private PlatformTransactionManager transactionManager;

    private SequentialInboxPendingActionsService service;

    @BeforeEach
    void setUp() {
        service = new SequentialInboxPendingActionsService(
                messageRepository,
                sequenceInstanceRepository,
                sequentialInboxService,
                new PendingActionsConfigProperties(),
                transactionManager
        );
    }

    @Test
    void runPendingActionsOnMessages_shouldProcessMessages() {
        SequencedMessage message1 = mock(SequencedMessage.class);
        SequencedMessage message2 = mock(SequencedMessage.class);
        Page<SequencedMessage> page = new PageImpl<>(List.of(message1, message2));
        when(messageRepository.getMessagesWithPendingAction(any())).thenReturn(page);

        service.runPendingActionsOnMessages();

        verify(sequentialInboxService).handleMessageWithPendingAction(message1);
        verify(sequentialInboxService).handleMessageWithPendingAction(message2);
    }

    @Test
    void runPendingActionsOnMessages_shouldNotProcessWhenEmpty() {
        when(messageRepository.getMessagesWithPendingAction(any())).thenReturn(Page.empty());

        service.runPendingActionsOnMessages();

        verify(sequentialInboxService, never()).handleMessageWithPendingAction(any());
    }

    @Test
    void runPendingActionsOnSequences_shouldProcessSequences() {
        SequenceInstance seq1 = mock(SequenceInstance.class);
        SequenceInstance seq2 = mock(SequenceInstance.class);
        Page<SequenceInstance> page = new PageImpl<>(List.of(seq1, seq2));
        when(sequenceInstanceRepository.findAllByPendingActionIsNotNull(any())).thenReturn(page);

        service.runPendingActionsOnSequences();

        verify(sequentialInboxService).handleSequenceWithPendingAction(seq1);
        verify(sequentialInboxService).handleSequenceWithPendingAction(seq2);
    }

    @Test
    void runPendingActionsOnSequences_shouldNotProcessWhenEmpty() {
        when(sequenceInstanceRepository.findAllByPendingActionIsNotNull(any())).thenReturn(Page.empty());

        service.runPendingActionsOnSequences();

        verify(sequentialInboxService, never()).handleSequenceWithPendingAction(any());
    }
}
