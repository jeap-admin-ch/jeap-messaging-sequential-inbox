package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping.SequentialInboxHousekeepingService;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class SequentialInboxHousekeepingServiceIT extends SequentialInboxITBase {

    private static final String TEST_MESSAGE_TYPE = "testMessageType";

    private final MessageRepository messageRepository;
    private final SequenceInstanceRepository sequenceInstanceRepository;
    private final SequentialInboxHousekeepingService housekeepingService;
    private final PlatformTransactionManager transactionManager;

    @Autowired
    SequentialInboxHousekeepingServiceIT(MessageRepository messageRepository,
                                         SequenceInstanceRepository sequenceInstanceRepository,
                                         SequentialInboxHousekeepingService housekeepingService,
                                         PlatformTransactionManager transactionManager) {
        this.messageRepository = messageRepository;
        this.sequenceInstanceRepository = sequenceInstanceRepository;
        this.housekeepingService = housekeepingService;
        this.transactionManager = transactionManager;
    }

    @Test
    void housekeepingClosedSequenceShouldBeDeleted() {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(Propagation.REQUIRES_NEW.value());
        String idempotenceIdMessage1 = UUID.randomUUID().toString();
        String idempotenceIdMessage2 = UUID.randomUUID().toString();

        transactionTemplate.executeWithoutResult(status -> {
            long sequenceInstanceId = saveSequenceInstance(UUID.randomUUID().toString(), SequenceInstanceState.CLOSED);
            saveMessage(TEST_MESSAGE_TYPE, sequenceInstanceId, idempotenceIdMessage1, true);

            long sequenceInstance2Id = saveSequenceInstance(UUID.randomUUID().toString(), SequenceInstanceState.OPEN);
            saveMessage(TEST_MESSAGE_TYPE, sequenceInstance2Id, idempotenceIdMessage2, true);
        });

        assertThat(messageRepository.findByMessageTypeAndIdempotenceIdInNewTransaction(TEST_MESSAGE_TYPE, idempotenceIdMessage1)).isPresent();

        housekeepingService.deleteClosedSequenceInstances();

        assertThat(messageRepository.findByMessageTypeAndIdempotenceIdInNewTransaction(TEST_MESSAGE_TYPE, idempotenceIdMessage1)).isEmpty();
        assertThat(messageRepository.findByMessageTypeAndIdempotenceIdInNewTransaction(TEST_MESSAGE_TYPE, idempotenceIdMessage2)).isPresent();
    }

    private long saveSequenceInstance(String contextId, SequenceInstanceState state) {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .contextId(contextId)
                .state(state)
                .name("test")
                .retentionPeriod(Duration.ofNanos(1))
                .build();
        return sequenceInstanceRepository.saveNewInstance(sequenceInstance);
    }

    private void saveMessage(String messageType, long sequenceInstanceId, String idempotenceId, boolean withHeaders) {
        BufferedMessage bufferedMessage = BufferedMessage.builder()
                .key("testKey".getBytes())
                .value("testValue".getBytes())
                .sequenceInstanceId(sequenceInstanceId)
                .build();

        if (withHeaders) {
            List<MessageHeader> headers = List.of(MessageHeader.builder()
                    .headerName("testHeader")
                    .headerValue("test".getBytes())
                    .bufferedMessage(bufferedMessage)
                    .build());
            bufferedMessage.setHeaders(headers);
        }

        SequencedMessage sequencedMessage = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .sequencedMessageId(UUID.randomUUID())
                .messageType(messageType)
                .state(SequencedMessageState.WAITING)
                .idempotenceId(idempotenceId)
                .clusterName("testCluster")
                .topic("testTopic")
                .build();

        messageRepository.saveMessage(bufferedMessage, sequencedMessage);
    }

}
