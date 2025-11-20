package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.*;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
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
    public List<SequencedMessage> getMessagesWithPendingAction() {
        return sequencedMessageRepository.findAllByPendingActionIsNotNull();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public BufferedMessage getBufferedMessageInNewTransaction(SequencedMessage sequencedMessage) {
        return bufferedMessageRepository.getBySequencedMessageId(sequencedMessage.getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void setMessageStateInNewTransaction(SequencedMessage sequencedMessage, SequencedMessageState sequencedMessageState) {
        sequencedMessageRepository.updateStateById(sequencedMessage.getId(), sequencedMessageState.name());
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
    public Map<String, Double> getWaitingMessageCountByType() {
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
    public Optional<SequencedMessage> findById(long id) {
        return sequencedMessageRepository.findById(id);
    }
}
