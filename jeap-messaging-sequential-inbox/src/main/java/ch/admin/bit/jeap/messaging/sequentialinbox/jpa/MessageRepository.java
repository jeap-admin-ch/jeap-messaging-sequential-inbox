package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.MessageHeader;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessageState;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MessageRepository {

    private static final Set<SequencedMessageState> WAITING_AND_PROCESSED_STATE = Set.of(SequencedMessageState.WAITING, SequencedMessageState.PROCESSED);

    private final SpringDataJpaBufferedMessageRepository bufferedMessageRepository;
    private final SpringDataJpaSequencedMessageRepository sequencedMessageRepository;
    private final SpringDataJpaMessageHeaderRepository messageHeaderRepository;

    private final EntityManager entityManager;

    @Transactional(propagation = Propagation.MANDATORY)
    public void saveMessage(BufferedMessage bufferedMessage, SequencedMessage sequencedMessage) {
        SequencedMessage persistentSequencedMessage = sequencedMessageRepository.save(sequencedMessage);
        if (bufferedMessage != null) {
            bufferedMessage.setSequencedMessageId(persistentSequencedMessage.getId());
            bufferedMessageRepository.save(bufferedMessage);
        }
        entityManager.flush();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public Set<String> getProcessedMessageTypesInSequenceInNewTransaction(long sequenceInstanceId) {
        return sequencedMessageRepository.getProcessedMessageTypesInSequence(sequenceInstanceId);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public List<SequencedMessage> getWaitingAndProcessedMessagesInNewTransaction(long sequenceInstanceId) {
        return sequencedMessageRepository.findAllBySequenceInstanceIdAndStateIn(sequenceInstanceId, WAITING_AND_PROCESSED_STATE);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public List<SequencedMessage> getWaitingMessagesInNewTransaction(long sequenceInstanceId) {
        return sequencedMessageRepository.findAllBySequenceInstanceIdAndStateIn(sequenceInstanceId, Set.of(SequencedMessageState.WAITING));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public Slice<SequencedMessage> getMessagesWithPendingAction(Pageable pageable) {
        return sequencedMessageRepository.findAllByPendingActionIsNotNull(pageable);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public BufferedMessage getBufferedMessageInNewTransaction(SequencedMessage sequencedMessage) {
        return bufferedMessageRepository.getBySequencedMessageId(sequencedMessage.getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void setMessageStateInNewTransaction(SequencedMessage sequencedMessage, SequencedMessageState sequencedMessageState) {
        sequencedMessageRepository.updateStateById(sequencedMessage.getId(), sequencedMessageState.name());
    }

    /**
     * Marks a buffered processing attempt as failed and releases its committed idempotence claim atomically.
     * A later delivery can therefore claim and retry the message, while no delivery can observe an available
     * claim before the FAILED state has been committed.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void markMessageFailedAndReleaseIdempotenceClaimInNewTransaction(SequencedMessage sequencedMessage) {
        sequencedMessageRepository.updateStateById(sequencedMessage.getId(), SequencedMessageState.FAILED.name());
        int deletedClaims = entityManager.createNativeQuery("""
                        DELETE FROM sequential_inbox_idempotence
                        WHERE message_type = ?1
                          AND idempotence_id = ?2
                        """)
                .setParameter(1, sequencedMessage.getMessageType())
                .setParameter(2, sequencedMessage.getIdempotenceId())
                .executeUpdate();
        if (deletedClaims == 0) {
            log.warn("No idempotence claim found while marking sequenced message {} of type {} with idempotence ID {} as FAILED. " +
                     "The message remains retryable, but the missing claim indicates previously modified or inconsistent Inbox state.",
                    sequencedMessage.getId(), sequencedMessage.getMessageType(), sequencedMessage.getIdempotenceId());
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void clearPendingActionInNewTransaction(SequencedMessage sequencedMessage) {
        sequencedMessageRepository.clearPendingActionById(sequencedMessage.getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void clearPendingActionInNewTransaction(SequencedMessage sequencedMessage, SequencedMessageState sequencedMessageState) {
        sequencedMessageRepository.clearPendingActionById(sequencedMessage.getId(), sequencedMessageState.name());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void setMessageStateInCurrentTransaction(SequencedMessage sequencedMessage, SequencedMessageState sequencedMessageState) {
        sequencedMessageRepository.updateStateById(sequencedMessage.getId(), sequencedMessageState.name());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public Optional<SequencedMessage> findByMessageTypeAndIdempotenceIdInNewTransaction(String messageType, String idempotenceId) {
        return sequencedMessageRepository.findByMessageTypeAndIdempotenceId(messageType, idempotenceId);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean createIdempotenceClaim(String messageType, String idempotenceId, long sequenceInstanceId) {
        int insertedRows = entityManager.createNativeQuery("""
                        INSERT INTO sequential_inbox_idempotence
                            (message_type, idempotence_id, sequence_instance_id, created_at)
                        VALUES (?1, ?2, ?3, NOW())
                        ON CONFLICT (message_type, idempotence_id) DO NOTHING
                        """)
                .setParameter(1, messageType)
                .setParameter(2, idempotenceId)
                .setParameter(3, sequenceInstanceId)
                .executeUpdate();
        return insertedRows == 1;
    }

    /**
     * Deletes all messages (sequenced, buffered, headers) associated with a given sequence instance
     * if the sequence instance is not in state CLOSED.
     * @return The number of sequenced messages deleted
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public int deleteNotClosedSequenceInstanceMessages(long sequenceInstanceId) {
        messageHeaderRepository.deleteForNotClosedSequencedId(sequenceInstanceId);
        bufferedMessageRepository.deleteForNotClosedSequencedId(sequenceInstanceId);
        return sequencedMessageRepository.deleteForNotClosedSequencedId(sequenceInstanceId);
    }

    /**
     * Deletes all messages (buffered and sequenced, message headers) associated with sequence instances in CLOSED state.
     * @return The number of sequenced messages deleted
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public int deleteMessagesForClosedSequences() {
        messageHeaderRepository.deleteForClosedSequences();
        bufferedMessageRepository.deleteForClosedSequences();
        return sequencedMessageRepository.deleteForClosedSequences();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Map<String, Long> getWaitingMessageCountByType() {
        return sequencedMessageRepository.getWaitingMessageCountGroupedByMessageType().stream()
                .collect(toMap(CountByType::messageType, CountByType::waitingCount));
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public Map<String, byte[]> getHeaders(SequencedMessage sequencedMessage) {
        List<MessageHeader> headers = messageHeaderRepository.getHeadersForSequencedMessageId(sequencedMessage.getId());
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        return headers.stream()
                .collect(toMap(MessageHeader::getHeaderName, MessageHeader::getHeaderValue));
    }

    @Transactional(readOnly = true)
    public List<SequencedMessage> findAllBySequenceInstanceId(long sequenceInstanceId) {
        return sequencedMessageRepository.findAllBySequenceInstanceId(sequenceInstanceId);
    }

    @Transactional(readOnly = true)
    public List<BufferedMessage> findAllBufferedMessagesBySequenceInstanceId(long sequenceInstanceId) {
        return bufferedMessageRepository.findAllBySequenceInstanceId(sequenceInstanceId);
    }

    @Transactional(readOnly = true)
    public Optional<SequencedMessage> findById(long id) {
        return sequencedMessageRepository.findById(id);
    }
}
