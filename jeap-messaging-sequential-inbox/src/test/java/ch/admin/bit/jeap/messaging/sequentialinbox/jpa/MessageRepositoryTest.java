package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.transaction.TestTransaction;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ContextConfiguration(classes = MessageRepositoryTest.TestConfig.class)
class MessageRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @EnableJpaRepositories
    @EntityScan(basePackageClasses = SequencedMessage.class)
    @ComponentScan
    static class TestConfig {
    }

    @Autowired
    private MessageRepository messageRepository;
    @Autowired
    private SequenceInstanceRepository sequenceInstanceRepository;

    @Autowired
    TestEntityManager testEntityManager;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @AfterEach
    void tearDown() {
        jdbcTemplate.execute("delete from message_header");
        jdbcTemplate.execute("delete from buffered_message");
        jdbcTemplate.execute("delete from sequenced_message");
        jdbcTemplate.execute("delete from sequence_instance");
    }

    @Test
    void saveMessage_withBufferedMessage() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        BufferedMessage bufferedMessage = BufferedMessage.builder()
                .sequenceInstanceId(1L)
                .key(new byte[]{1, 2, 3})
                .value(new byte[]{4, 5, 6})
                .sequenceInstanceId(sequenceInstanceId)
                .build();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);

        messageRepository.saveMessage(bufferedMessage, sequencedMessage);

        assertThat(bufferedMessage.getSequencedMessageId())
                .isEqualTo(sequencedMessage.getId());
        assertThat(testEntityManager.find(BufferedMessage.class, bufferedMessage.getId()))
                .isNotNull();
        assertThat(testEntityManager.find(SequencedMessage.class, sequencedMessage.getId()))
                .isNotNull();
    }

    @Test
    void saveMessage_withoutBufferedMessage() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);

        messageRepository.saveMessage(null, sequencedMessage);

        assertThat(testEntityManager.find(SequencedMessage.class, sequencedMessage.getId()))
                .isNotNull();
    }

    @Test
    void getProcessedMessageTypesInSequenceInNewTransaction_returnsCorrectTypes() {
        long sequenceInstanceId = createAndPersistSequenceInstance();

        SequencedMessage sequencedMessage1 = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type1")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId1")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.PROCESSED)
                .build();
        SequencedMessage sequencedMessage2 = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type2")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId2")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.PROCESSED)
                .build();
        messageRepository.saveMessage(null, sequencedMessage1);
        messageRepository.saveMessage(null, sequencedMessage2);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        Set<String> result = messageRepository.getProcessedMessageTypesInSequenceInNewTransaction(sequenceInstanceId);

        assertThat(result).contains("type1", "type2");
    }

    @Test
    void getWaitingAndProcessedMessagesInNewTransaction_returnsCorrectMessages() {
        long sequenceInstanceId = createAndPersistSequenceInstance();

        SequencedMessage sequencedMessage1 = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type1")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId1")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.WAITING)
                .build();
        SequencedMessage sequencedMessage2 = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type2")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId2")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.PROCESSED)
                .build();
        messageRepository.saveMessage(null, sequencedMessage1);
        messageRepository.saveMessage(null, sequencedMessage2);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        List<SequencedMessage> result = messageRepository.getWaitingAndProcessedMessagesInNewTransaction(sequenceInstanceId);

        assertThat(result).contains(sequencedMessage1, sequencedMessage2);
    }

    @Test
    void getBufferedMessageInNewTransaction_returnsCorrectMessage() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.WAITING)
                .build();
        BufferedMessage bufferedMessage = BufferedMessage.builder()
                .sequenceInstanceId(1L)
                .key(new byte[]{1, 2, 3})
                .value(new byte[]{4, 5, 6})
                .sequenceInstanceId(sequenceInstanceId)
                .build();
        bufferedMessage.setHeaders(List.of(
                MessageHeader.builder()
                        .bufferedMessage(bufferedMessage)
                        .headerName("name")
                        .headerValue("value".getBytes(UTF_8))
                        .build()
        ));
        messageRepository.saveMessage(bufferedMessage, sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        TestTransaction.start();
        BufferedMessage result = messageRepository.getBufferedMessageInNewTransaction(sequencedMessage);

        assertThat(result).isEqualTo(bufferedMessage);
        assertThat(result.getHeaderMap())
                .hasSize(1)
                .containsEntry("name", "value".getBytes(UTF_8));

        Map<String, byte[]> headers = messageRepository.getHeaders(sequencedMessage);
        assertThat(headers)
                .hasSize(1)
                .containsEntry("name", "value".getBytes(StandardCharsets.UTF_8));
        TestTransaction.end();
    }

    @Test
    void setMessageStateInNewTransaction_updatesStateCorrectly() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        testEntityManager.persist(sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        messageRepository.setMessageStateInNewTransaction(sequencedMessage, SequencedMessageState.PROCESSED);

        TestTransaction.start();
        SequencedMessage updatedMessage = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updatedMessage.getState()).isEqualTo(SequencedMessageState.PROCESSED);
    }

    @Test
    void setMessageStateInCurrentTransaction_updatesStateCorrectly() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        messageRepository.saveMessage(null, sequencedMessage);

        messageRepository.setMessageStateInCurrentTransaction(sequencedMessage, SequencedMessageState.PROCESSED);

        testEntityManager.clear();
        SequencedMessage updatedMessage = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updatedMessage.getState()).isEqualTo(SequencedMessageState.PROCESSED);
    }

    @Test
    void findByMessageTypeAndIdempotenceId_returnsCorrectMessageInNewTransaction() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        messageRepository.saveMessage(null, sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        Optional<SequencedMessage> result = messageRepository.findByMessageTypeAndIdempotenceIdInNewTransaction("type", "idempotenceId");

        assertThat(result)
                .isPresent()
                .contains(sequencedMessage);
    }

    @Test
    void getWaitingMessageCountByType() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage waitingMessage = createSequencedMessage(sequenceInstanceId);
        SequencedMessage processedMessage = createSequencedMessage(sequenceInstanceId);
        SequencedMessage otherTypeWaitingMessage = createSequencedMessage(sequenceInstanceId, "otherType");
        processedMessage.setState(SequencedMessageState.PROCESSED);
        messageRepository.saveMessage(null, waitingMessage);
        messageRepository.saveMessage(null, otherTypeWaitingMessage);
        messageRepository.saveMessage(null, processedMessage);

        Map<String, Double> countByType = messageRepository.getWaitingMessageCountByType();

        assertThat(countByType)
                .containsEntry("type", 1.0)
                .containsEntry("otherType", 1.0)
                .hasSize(2);
    }

    @Test
    void getWaitingMessagesInNewTransaction_returnsOnlyWaitingMessages() {
        long sequenceInstanceId = createAndPersistSequenceInstance();

        SequencedMessage waitingMessage1 = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type1")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId1")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.WAITING)
                .build();
        SequencedMessage waitingMessage2 = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type2")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId2")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.WAITING)
                .build();
        SequencedMessage processedMessage = SequencedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .messageType("type3")
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId3")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.PROCESSED)
                .build();
        messageRepository.saveMessage(null, waitingMessage1);
        messageRepository.saveMessage(null, waitingMessage2);
        messageRepository.saveMessage(null, processedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        List<SequencedMessage> result = messageRepository.getWaitingMessagesInNewTransaction(sequenceInstanceId);

        assertThat(result)
                .hasSize(2)
                .contains(waitingMessage1, waitingMessage2)
                .doesNotContain(processedMessage);
    }

    @Test
    void deleteNotClosedSequenceInstanceMessages_doesDeleteMessagesForNotClosedSequence() {
        // Create an OPEN (not closed) sequence instance with messages
        long openSequenceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(openSequenceId);
        BufferedMessage bufferedMessage = createBufferedMessageWithOneHeader(openSequenceId);
        messageRepository.saveMessage(bufferedMessage, sequencedMessage);
        Long headerId = bufferedMessage.getHeaders().getFirst().getId();

        int deletedCount = messageRepository.deleteNotClosedSequenceInstanceMessages(openSequenceId);

        // Should delete 1 sequenced message
        assertThat(deletedCount).isEqualTo(1);

        // Verify all entities are deleted
        testEntityManager.clear(); // Clear the persistence context to ensure we fetch fresh data
        assertThat(testEntityManager.find(SequencedMessage.class, sequencedMessage.getId())).isNull();
        assertThat(testEntityManager.find(BufferedMessage.class, bufferedMessage.getId())).isNull();
        assertThat(testEntityManager.find(MessageHeader.class, headerId)).isNull();
    }

    @Test
    void deleteNotClosedSequenceInstanceMessages_doesNotDeleteMessagesForClosedSequence() {
        // Create a CLOSED sequence instance with messages
        SequenceInstance closedSequence = SequenceInstance.builder()
                .name("closedTest")
                .contextId(UUID.randomUUID().toString())
                .state(SequenceInstanceState.CLOSED)
                .retentionPeriod(Duration.ofDays(7))
                .build();
        long closedSequenceId = sequenceInstanceRepository.saveNewInstance(closedSequence);
        SequencedMessage sequencedMessage = createSequencedMessage(closedSequenceId);
        BufferedMessage bufferedMessage = createBufferedMessageWithOneHeader(closedSequenceId);
        messageRepository.saveMessage(bufferedMessage, sequencedMessage);
        Long headerId = bufferedMessage.getHeaders().getFirst().getId();

        int deletedCount = messageRepository.deleteNotClosedSequenceInstanceMessages(closedSequenceId);

        // Should not delete anything
        assertThat(deletedCount).isZero();

        // Verify entities still exist
        testEntityManager.clear(); // Clear the persistence context to ensure we fetch fresh data
        assertThat(testEntityManager.find(SequencedMessage.class, sequencedMessage.getId())).isNotNull();
        assertThat(testEntityManager.find(BufferedMessage.class, bufferedMessage.getId())).isNotNull();
        assertThat(testEntityManager.find(MessageHeader.class, headerId)).isNotNull();
    }

    @Test
    void deleteNotClosedSequenceInstanceMessages_deletesMultipleMessages() {
        long sequenceId = createAndPersistSequenceInstance();

        // Create multiple messages for the same sequence
        SequencedMessage message1 = createSequencedMessage(sequenceId);
        SequencedMessage message2 = createSequencedMessage(sequenceId);
        SequencedMessage message3 = createSequencedMessage(sequenceId);

        messageRepository.saveMessage(null, message1);
        messageRepository.saveMessage(null, message2);
        messageRepository.saveMessage(null, message3);

        int deletedCount = messageRepository.deleteNotClosedSequenceInstanceMessages(sequenceId);

        // Should delete all 3 messages
        assertThat(deletedCount).isEqualTo(3);

        // Verify all are deleted
        testEntityManager.clear(); // Clear the persistence context to ensure we fetch fresh data
        assertThat(testEntityManager.find(SequencedMessage.class, message1.getId())).isNull();
        assertThat(testEntityManager.find(SequencedMessage.class, message2.getId())).isNull();
        assertThat(testEntityManager.find(SequencedMessage.class, message3.getId())).isNull();
    }

    private static SequencedMessage createSequencedMessage(long instanceId) {
        return createSequencedMessage(instanceId, "type");
    }

    private static SequencedMessage createSequencedMessage(long instanceId, String type) {
        return SequencedMessage.builder()
                .sequenceInstanceId(instanceId)
                .messageType(type)
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId("idempotenceId")
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.WAITING)
                .build();
    }

    private long createAndPersistSequenceInstance() {
        return sequenceInstanceRepository.saveNewInstance(SequenceInstance.builder()
                .name("test")
                .contextId(UUID.randomUUID().toString())
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofDays(7))
                .build());
    }

    private BufferedMessage createBufferedMessageWithOneHeader(long sequenceInstanceId) {
        BufferedMessage bufferedMessage = BufferedMessage.builder()
                .sequenceInstanceId(sequenceInstanceId)
                .key(new byte[]{1, 2, 3})
                .value(new byte[]{4, 5, 6})
                .sequenceInstanceId(sequenceInstanceId)
                .build();
        bufferedMessage.setHeaders(List.of(
                MessageHeader.builder()
                        .bufferedMessage(bufferedMessage)
                        .headerName("name")
                        .headerValue("value".getBytes(UTF_8))
                        .build()
        ));
        return bufferedMessage;
    }

}
