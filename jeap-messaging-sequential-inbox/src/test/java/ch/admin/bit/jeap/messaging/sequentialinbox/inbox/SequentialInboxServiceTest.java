package ch.admin.bit.jeap.messaging.sequentialinbox.inbox;

import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.Sequence;
import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SequencedMessageType;
import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SequentialInboxConfiguration;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.spring.SequentialInboxMessageHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SequentialInboxServiceTest {

    @Mock
    private SequenceInstanceFactory sequenceInstanceFactory;
    @Mock
    private SequencedMessageService sequencedMessageService;
    @Mock
    private SequenceInstanceRepository sequenceInstanceRepository;
    @Mock
    private SequentialInboxConfiguration inboxConfiguration;
    @Mock
    private Transactions transactions;
    @Mock
    private MessageHandlerService messageHandlerService;
    @Mock
    private BufferedMessageService bufferedMessageService;
    @Mock
    private SequentialInboxMessageHandler messageHandler;
    @Mock
    private Acknowledgment acknowledgment;
    @Mock
    private SequencedMessageType sequencedMessageType;
    @Mock
    private Sequence sequence;
    @Mock
    private SequenceInstance sequenceInstance;

    private SequentialInboxService service;

    @BeforeEach
    void setUp() {
        service = new SequentialInboxService(sequenceInstanceFactory, sequencedMessageService,
                sequenceInstanceRepository, inboxConfiguration, transactions, messageHandlerService,
                bufferedMessageService);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(transactions).runInNewTransaction(any());
    }

    @Test
    void handleMessage_whenIdempotenceClaimAlreadyExists_skipsProcessingAndAcknowledgesRecord() {
        AvroMessage message = mock(AvroMessage.class, RETURNS_DEEP_STUBS);
        when(message.getIdentity().getId()).thenReturn("message-id");
        when(message.getIdentity().getIdempotenceId()).thenReturn("idempotence-id");
        when(inboxConfiguration.qualifiedSequencedMessageTypeName(message)).thenReturn("qualified-message-type");
        when(inboxConfiguration.requireSequencedMessageTypeByQualifiedName("qualified-message-type"))
                .thenReturn(sequencedMessageType);
        when(sequencedMessageType.shouldSequenceMessage(message)).thenReturn(true);
        when(sequencedMessageType.extractContextId(message)).thenReturn("context-id");
        when(inboxConfiguration.getSequenceByQualifiedSequencedMessageTypeName("qualified-message-type"))
                .thenReturn(sequence);
        when(sequence.getName()).thenReturn("sequence-name");
        when(sequenceInstanceFactory.createOrGetSequenceInstance(sequence, "context-id")).thenReturn(42L);
        when(sequencedMessageService.createIdempotenceClaim(
                "qualified-message-type", "idempotence-id", 42L)).thenReturn(false);
        when(sequenceInstanceFactory.getExistingSequenceInstanceAndLockForUpdate(42L)).thenReturn(sequenceInstance);

        ConsumerRecord<AvroMessageKey, AvroMessage> consumerRecord =
                new ConsumerRecord<>("topic", 0, 1L, null, message);

        service.handleMessage(consumerRecord, messageHandler, acknowledgment);

        verify(messageHandler, never()).invoke(any(), any());
        verify(sequencedMessageService, never()).findByMessageTypeAndIdempotenceId(any(), any());
        verify(acknowledgment).acknowledge();
    }
}
