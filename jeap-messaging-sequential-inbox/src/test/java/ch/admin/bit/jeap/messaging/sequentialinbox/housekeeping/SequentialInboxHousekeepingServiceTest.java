package ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping;

import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.ErrorHandlingService;
import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.Transactions;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = SequentialInboxHousekeepingServiceTest.TestConfig.class)
@TestPropertySource(properties = {
        "jeap.messaging.sequential-inbox.housekeeping.expiry-cron=*/2 * * * * ?", // Run every 2 seconds
        "jeap.messaging.sequential-inbox.housekeeping.closed-instances-cron=*/2 * * * * ?", // Run every 2 seconds
        "jeap.messaging.sequential-inbox.housekeeping.delete-for-removal-cron=-", // explicitly started in test
        "jeap.messaging.sequential-inbox.housekeeping.delay=1s", // small delay for testing
        "jeap.messaging.sequential-inbox.housekeeping.sequence-removal-batch-size=2", // small batch for testing
        "logging.level.ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping.SequentialInboxHousekeepingService=DEBUG"
})
class SequentialInboxHousekeepingServiceTest {

    @Configuration
    @EnableScheduling
    @EnableConfigurationProperties(HouseKeepingConfigProperties.class)
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    static class TestConfig {
        @Bean
        public SequentialInboxHousekeepingService housekeepingService(MessageRepository messageRepository,
                                                                      SequenceInstanceRepository sequenceInstanceRepository,
                                                                      HouseKeepingConfigProperties configProperties,
                                                                      ErrorHandlingService errorHandlingService,
                                                                      Transactions transactions) {
            return new SequentialInboxHousekeepingService(messageRepository, sequenceInstanceRepository,
                    configProperties, errorHandlingService, transactions);
        }

        @Bean
        Transactions transactions(PlatformTransactionManager platformTransactionManager) {
            return new Transactions(platformTransactionManager) {
            };
        }
    }

    @Autowired
    private SequentialInboxHousekeepingService housekeepingService;

    @MockitoBean
    private MessageRepository messageRepository;

    @MockitoBean
    private SequenceInstanceRepository sequenceInstanceRepository;

    @MockitoBean
    private ErrorHandlingService errorHandlingService;

    @MockitoBean
    private PlatformTransactionManager platformTransactionManager;

    @Test
    void shouldAutomaticallyDeleteClosedSequenceInstancesOnSchedule() {
        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    verify(messageRepository, atLeast(1)).deleteMessagesForClosedSequences();
                    verify(sequenceInstanceRepository, atLeast(1)).deleteAllClosed();
                });
    }

    @Test
    void shouldAutomaticallyMarkExpiredSequencesForRemovalOnSchedule() {
        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(sequenceInstanceRepository, atLeast(1)).
                        markExpiredInstancesForDelayedRemoval(1L));
    }

    @Test
    void shouldDeleteSequencesReadyForRemovalAndSendMessagesToErrorHandler() {
        // Create mocked sequence instances ready for removal
        final long readyForRemovalInstanceSequenceId = 42L;
        final long noBufferedMessagesInstanceSequenceId = 43L;
        final long noMessagesInstanceSequenceId = 44L;
        SequenceInstance readyForRemovalInstance = mockSequenceInstance(readyForRemovalInstanceSequenceId);
        SequenceInstance noBufferedMessagesInstance = mockSequenceInstance(noBufferedMessagesInstanceSequenceId);
        SequenceInstance noMessagesInstance = mockSequenceInstance(noMessagesInstanceSequenceId);
        SequencedMessage readyForRemovalInstanceSequencedMessage1 = mock(SequencedMessage.class);
        SequencedMessage readyForRemovalInstanceSequencedMessage2 = mock(SequencedMessage.class);
        BufferedMessage readyForRemovalInstanceBufferedMessage1 = mock(BufferedMessage.class);
        BufferedMessage readyForRemovalInstanceBufferedMessage2 = mock(BufferedMessage.class);
        SequencedMessage noBufferedMessagesInstanceSequencedMessage1 = mock(SequencedMessage.class);
        SequencedMessage noBufferedMessagesInstanceSequencedMessage2 = mock(SequencedMessage.class);
        SequencedMessage noBufferedMessagesInstanceSequencedMessage3 = mock(SequencedMessage.class);

        when(sequenceInstanceRepository.findInstancesForRemovalOldestFirst(anyInt()))
                .thenReturn(List.of(noBufferedMessagesInstance, noMessagesInstance)) // first batch with two instances
                .thenReturn(List.of(readyForRemovalInstance)) // second batch with one instance
                .thenReturn(List.of()); // empty batch to stop processing

        when(messageRepository.getWaitingMessagesInNewTransaction(readyForRemovalInstanceSequenceId))
                .thenReturn(List.of(readyForRemovalInstanceSequencedMessage1, readyForRemovalInstanceSequencedMessage2));
        when(messageRepository.getWaitingMessagesInNewTransaction(noBufferedMessagesInstanceSequenceId))
                .thenReturn(List.of(noBufferedMessagesInstanceSequencedMessage1, noBufferedMessagesInstanceSequencedMessage2, noBufferedMessagesInstanceSequencedMessage3));
        when(messageRepository.getBufferedMessageInNewTransaction(readyForRemovalInstanceSequencedMessage1))
                .thenReturn(readyForRemovalInstanceBufferedMessage1);
        when(messageRepository.getBufferedMessageInNewTransaction(readyForRemovalInstanceSequencedMessage2))
                .thenReturn(readyForRemovalInstanceBufferedMessage2);
        when(messageRepository.deleteNotClosedSequenceInstanceMessages(readyForRemovalInstanceSequenceId))
                .thenReturn(2);
        when(sequenceInstanceRepository.deleteNotClosedById(readyForRemovalInstanceSequenceId))
                .thenReturn(1);
        when(messageRepository.deleteNotClosedSequenceInstanceMessages(noBufferedMessagesInstanceSequenceId))
                .thenReturn(3);
        when(sequenceInstanceRepository.deleteNotClosedById(noBufferedMessagesInstanceSequenceId))
                .thenReturn(1);
        when(messageRepository.deleteNotClosedSequenceInstanceMessages(noMessagesInstanceSequenceId))
                .thenReturn(0);
        when(sequenceInstanceRepository.deleteNotClosedById(noMessagesInstanceSequenceId))
                .thenReturn(1);

        // Execute
        housekeepingService.deleteSequencesReadyForRemoval();

        // Verify that sequence instances ready for removal were fetched (first batch, second batch, empty batch)
        verify(sequenceInstanceRepository, times(3)).findInstancesForRemovalOldestFirst(anyInt());

        // Verify that all messages associated with the readyForRemovalInstanceSequence were fetched
        verify(messageRepository).getWaitingMessagesInNewTransaction(readyForRemovalInstanceSequenceId);
        verify(messageRepository).getBufferedMessageInNewTransaction(readyForRemovalInstanceSequencedMessage1);
        verify(messageRepository).getBufferedMessageInNewTransaction(readyForRemovalInstanceSequencedMessage2);

        // Verify that all buffered messages of the sequences were sent to the error handler
        verify(errorHandlingService).sendDeletedSequencedMessageToErrorHandler(
                readyForRemovalInstance, readyForRemovalInstanceSequencedMessage1, readyForRemovalInstanceBufferedMessage1);
        verify(errorHandlingService).sendDeletedSequencedMessageToErrorHandler(
                readyForRemovalInstance, readyForRemovalInstanceSequencedMessage2, readyForRemovalInstanceBufferedMessage2);

        // Verify that the messages for all sequences ready for removal were deleted
        verify(messageRepository).deleteNotClosedSequenceInstanceMessages(readyForRemovalInstanceSequenceId);
        verify(messageRepository).deleteNotClosedSequenceInstanceMessages(noBufferedMessagesInstanceSequenceId);
        verify(messageRepository).deleteNotClosedSequenceInstanceMessages(noMessagesInstanceSequenceId);

        // Verify that all sequences ready for removal were deleted
        verify(sequenceInstanceRepository).deleteNotClosedById(readyForRemovalInstanceSequenceId);
        verify(sequenceInstanceRepository).deleteNotClosedById(noBufferedMessagesInstanceSequenceId);
        verify(sequenceInstanceRepository).deleteNotClosedById(noMessagesInstanceSequenceId);
    }

    private SequenceInstance mockSequenceInstance(long id) {
        SequenceInstance instance = mock(SequenceInstance.class);
        when(instance.getId()).thenReturn(id);
        return instance;
    }

}
