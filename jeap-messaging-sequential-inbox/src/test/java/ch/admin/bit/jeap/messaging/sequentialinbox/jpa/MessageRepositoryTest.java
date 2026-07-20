package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.transaction.TestTransaction;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@ContextConfiguration(classes = MessageRepositoryTest.TestConfig.class)
class MessageRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @EnableJpaRepositories
    @EntityScan(basePackageClasses = SequencedMessage.class)
    @ComponentScan
    static class TestConfig {
    }

    private final MessageRepository messageRepository;
    private final SequenceInstanceRepository sequenceInstanceRepository;
    private final TestEntityManager testEntityManager;
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    MessageRepositoryTest(MessageRepository messageRepository,
                         SequenceInstanceRepository sequenceInstanceRepository,
                         TestEntityManager testEntityManager,
                         JdbcTemplate jdbcTemplate) {
        this.messageRepository = messageRepository;
        this.sequenceInstanceRepository = sequenceInstanceRepository;
        this.testEntityManager = testEntityManager;
        this.jdbcTemplate = jdbcTemplate;
    }
    @Autowired
    PlatformTransactionManager transactionManager;
    @AfterEach
    void tearDown() {
        jdbcTemplate.execute("delete from message_header");
        jdbcTemplate.execute("delete from buffered_message");
        jdbcTemplate.execute("delete from sequenced_message");
        jdbcTemplate.execute("delete from sequential_inbox_idempotence");
        jdbcTemplate.execute("delete from sequence_instance");
    }

    @Test
    void saveMessageWithBufferedMessage() {
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
    void saveMessageWithoutBufferedMessage() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);

        messageRepository.saveMessage(null, sequencedMessage);

        assertThat(testEntityManager.find(SequencedMessage.class, sequencedMessage.getId()))
                .isNotNull();
    }

    @Test
    void schemaKeepsExistingSequencedMessageIdempotenceIndexNonUnique() {
        String indexDefinition = jdbcTemplate.queryForObject("""
                SELECT indexdef
                FROM pg_indexes
                WHERE tablename = 'sequenced_message'
                  AND indexname = 'sequenced_message_idempotence_id'
                """, String.class);

        assertThat(indexDefinition)
                .contains("CREATE INDEX")
                .doesNotContain("CREATE UNIQUE INDEX");
    }

    @Test
    void createIdempotenceClaim_isGlobalForMessageTypeAcrossSequenceInstances() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        long otherSequenceInstanceId = createAndPersistSequenceInstance();
        String messageType = "messageType";
        String idempotenceId = UUID.randomUUID().toString();

        boolean firstClaim = messageRepository.createIdempotenceClaim(
                messageType, idempotenceId, sequenceInstanceId);
        boolean duplicateClaim = messageRepository.createIdempotenceClaim(
                messageType, idempotenceId, otherSequenceInstanceId);
        boolean otherMessageTypeClaim = messageRepository.createIdempotenceClaim(
                "otherMessageType", idempotenceId, sequenceInstanceId);

        assertThat(firstClaim).isTrue();
        assertThat(duplicateClaim).isFalse();
        assertThat(otherMessageTypeClaim).isTrue();
    }

    @Test
    void createIdempotenceClaim_canBeAcquiredAgainAfterRollback() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        TestTransaction.flagForCommit();
        TestTransaction.end();

        TestTransaction.start();
        assertThat(messageRepository.createIdempotenceClaim(
                "messageType", "idempotenceId", sequenceInstanceId)).isTrue();
        TestTransaction.flagForRollback();
        TestTransaction.end();

        TestTransaction.start();
        assertThat(messageRepository.createIdempotenceClaim(
                "messageType", "idempotenceId", sequenceInstanceId)).isTrue();
    }

    @Test
    void createIdempotenceClaim_concurrentClaimWaitsAndLosesAfterWinnerCommits() throws Exception {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        String idempotenceId = UUID.randomUUID().toString();
        TestTransaction.flagForCommit();
        TestTransaction.end();

        CountDownLatch winnerHasClaim = new CountDownLatch(1);
        CountDownLatch winnerMayCommit = new CountDownLatch(1);
        CompletableFuture<Boolean> winner = CompletableFuture.supplyAsync(() -> inNewTransaction(() -> {
            boolean claimCreated = messageRepository.createIdempotenceClaim(
                    "messageType", idempotenceId, sequenceInstanceId);
            winnerHasClaim.countDown();
            await(winnerMayCommit);
            return claimCreated;
        }));

        assertThat(winnerHasClaim.await(5, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Boolean> loser = CompletableFuture.supplyAsync(() -> inNewTransaction(() ->
                messageRepository.createIdempotenceClaim(
                        "messageType", idempotenceId, sequenceInstanceId)));

        assertThatThrownBy(() -> loser.get(200, TimeUnit.MILLISECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        winnerMayCommit.countDown();

        assertThat(winner.get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(loser.get(5, TimeUnit.SECONDS)).isFalse();
        TestTransaction.start();
    }

    @Test
    void getProcessedMessageTypesInSequenceInNewTransactionReturnsCorrectTypes() {
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
    void getWaitingAndProcessedMessagesInNewTransactionReturnsCorrectMessages() {
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
    void getBufferedMessageInNewTransactionReturnsCorrectMessage() {
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
    void setMessageStateInNewTransactionUpdatesStateCorrectly() {
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
    void markMessageFailedAndReleaseIdempotenceClaim_releasesClaimByPrimaryKey() {
        long claimSequenceInstanceId = createAndPersistSequenceInstance();
        long messageSequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(messageSequenceInstanceId);
        testEntityManager.persist(sequencedMessage);
        assertThat(messageRepository.createIdempotenceClaim(
                sequencedMessage.getMessageType(), sequencedMessage.getIdempotenceId(), claimSequenceInstanceId))
                .isTrue();
        TestTransaction.flagForCommit();
        TestTransaction.end();

        messageRepository.markMessageFailedAndReleaseIdempotenceClaimInNewTransaction(sequencedMessage);

        TestTransaction.start();
        SequencedMessage updatedMessage = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updatedMessage.getState()).isEqualTo(SequencedMessageState.FAILED);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*) FROM sequential_inbox_idempotence
                WHERE message_type = ? AND idempotence_id = ?
                """, Integer.class, sequencedMessage.getMessageType(), sequencedMessage.getIdempotenceId()))
                .isZero();
        assertThat(messageRepository.createIdempotenceClaim(
                sequencedMessage.getMessageType(), sequencedMessage.getIdempotenceId(), messageSequenceInstanceId))
                .isTrue();
    }

    @Test
    void markMessageFailedAndReleaseIdempotenceClaim_keepsFailedStateWhenClaimIsAlreadyAbsent() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        testEntityManager.persist(sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        messageRepository.markMessageFailedAndReleaseIdempotenceClaimInNewTransaction(sequencedMessage);

        TestTransaction.start();
        SequencedMessage updatedMessage = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updatedMessage.getState()).isEqualTo(SequencedMessageState.FAILED);
    }

    @Test
    void setMessageStateInCurrentTransactionUpdatesStateCorrectly() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        messageRepository.saveMessage(null, sequencedMessage);

        messageRepository.setMessageStateInCurrentTransaction(sequencedMessage, SequencedMessageState.PROCESSED);

        testEntityManager.clear();
        SequencedMessage updatedMessage = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updatedMessage.getState()).isEqualTo(SequencedMessageState.PROCESSED);
    }

    @Test
    void findByMessageTypeAndIdempotenceIdReturnsCorrectMessageInNewTransaction() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        messageRepository.saveMessage(null, sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        Optional<SequencedMessage> result = messageRepository.findByMessageTypeAndIdempotenceIdInNewTransaction(
                sequencedMessage.getMessageType(), sequencedMessage.getIdempotenceId());

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

        Map<String, Long> countByType = messageRepository.getWaitingMessageCountByType();

        assertThat(countByType)
                .containsEntry("type", 1L)
                .containsEntry("otherType", 1L)
                .hasSize(2);
    }

    @Test
    void getWaitingMessagesInNewTransactionReturnsOnlyWaitingMessages() {
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
    void deleteNotClosedSequenceInstanceMessagesDoesDeleteMessagesForNotClosedSequence() {
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
    void deleteNotClosedSequenceInstanceMessagesDoesNotDeleteMessagesForClosedSequence() {
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
    void deleteNotClosedSequenceInstanceMessagesDeletesMultipleMessages() {
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

    @Test
    void clearPendingActionInNewTransactionClearsPendingAction() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        ReflectionTestUtils.setField(sequencedMessage, "pendingAction", SequencedMessagePendingAction.CONSUME);
        messageRepository.saveMessage(null, sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        messageRepository.clearPendingActionInNewTransaction(sequencedMessage);

        TestTransaction.start();
        SequencedMessage updated = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updated.getPendingAction()).isNull();
        TestTransaction.end();
    }

    @Test
    void clearPendingActionInNewTransactionClearsPendingActionAndSetState() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessage(sequenceInstanceId);
        ReflectionTestUtils.setField(sequencedMessage, "pendingAction", SequencedMessagePendingAction.CONSUME);
        messageRepository.saveMessage(null, sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        messageRepository.clearPendingActionInNewTransaction(sequencedMessage, SequencedMessageState.PROCESSED);

        TestTransaction.start();
        SequencedMessage updated = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(updated.getPendingAction()).isNull();
        assertThat(updated.getState()).isEqualTo(SequencedMessageState.PROCESSED);
        TestTransaction.end();
    }

    @Test
    void traceContextRoundTripsNullWhenNoneWasCapturedAtPersistTime() {
        // The null-trace-context contract is what BufferedMessageTracing relies on: if no span was active at
        // capture time, the embeddable must come back null on read so replay does not activate a synthetic
        // zeroed SpanContext. Hibernate's default for @Embedded with all-null columns is to materialize null,
        // governed by hibernate.create_empty_composites.enabled (default false).
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage sequencedMessage = createSequencedMessageWithTraceContext(sequenceInstanceId, null);
        messageRepository.saveMessage(null, sequencedMessage);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        TestTransaction.start();
        SequencedMessage reloaded = testEntityManager.find(SequencedMessage.class, sequencedMessage.getId());
        assertThat(reloaded.getTraceContext()).isNull();
        TestTransaction.end();
    }

    @Test
    void getMessagesWithPendingActionReturnsMessagesWithPendingAction() {
        long sequenceInstanceId = createAndPersistSequenceInstance();
        SequencedMessage messageWithPendingAction = createSequencedMessage(sequenceInstanceId);
        ReflectionTestUtils.setField(messageWithPendingAction, "pendingAction", SequencedMessagePendingAction.CONSUME);
        SequencedMessage messageWithoutPendingAction = createSequencedMessage(sequenceInstanceId);

        messageRepository.saveMessage(null, messageWithPendingAction);
        messageRepository.saveMessage(null, messageWithoutPendingAction);
        TestTransaction.flagForCommit();
        TestTransaction.end();

        PageRequest pageRequest = PageRequest.of(0, 10);
        Slice<SequencedMessage> result = messageRepository.getMessagesWithPendingAction(pageRequest);

        assertThat(result)
                .extracting(SequencedMessage::getId)
                .contains(messageWithPendingAction.getId())
                .doesNotContain(messageWithoutPendingAction.getId());
    }

    private static SequencedMessage createSequencedMessage(long instanceId) {
        return createSequencedMessage(instanceId, "type");
    }

    private static SequencedMessage createSequencedMessage(long instanceId, String type) {
        return createSequencedMessage(instanceId, type, null);
    }

    @SuppressWarnings("SameParameterValue")
    private static SequencedMessage createSequencedMessageWithTraceContext(long instanceId, SequentialInboxTraceContext traceContext) {
        return createSequencedMessage(instanceId, "type", traceContext);
    }

    private static SequencedMessage createSequencedMessage(long instanceId, String type, SequentialInboxTraceContext traceContext) {
        return SequencedMessage.builder()
                .sequenceInstanceId(instanceId)
                .messageType(type)
                .sequencedMessageId(UUID.randomUUID())
                .idempotenceId(UUID.randomUUID().toString())
                .clusterName("cluster")
                .topic("topic")
                .state(SequencedMessageState.WAITING)
                .traceContext(traceContext)
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

    private <T> T inNewTransaction(Supplier<T> supplier) {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        return transaction.execute(ignored -> supplier.get());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for concurrent claim test");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for concurrent claim test", e);
        }
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
