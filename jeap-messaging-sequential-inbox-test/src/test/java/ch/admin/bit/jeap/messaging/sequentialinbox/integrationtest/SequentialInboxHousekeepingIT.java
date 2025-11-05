package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.errorevent.FailedMessageMetadata;
import ch.admin.bit.jeap.messaging.avro.errorevent.MessageProcessingFailedEvent;
import ch.admin.bit.jeap.messaging.avro.errorevent.MessageProcessingFailedPayload;
import ch.admin.bit.jeap.messaging.kafka.signature.SignatureHeaders;
import ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping.SequentialInboxHousekeepingService;
import ch.admin.bit.jme.declaration.JmeDeclarationCreatedEvent;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class SequentialInboxHousekeepingIT extends SequentialInboxITBase {

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private SequentialInboxHousekeepingService housekeepingService;

    @Test
    void testHousekeeping_deleteSequencesReadyForRemoval() {
        // given: two open sequences contextIdExpired and contextIdForRemoval
        // and the buffered message value of contextIdForRemoval
        final UUID contextIdExpired = randomContextId();
        final UUID contextIdForRemoval = randomContextId();
        createOpenSequence(contextIdExpired);
        final AvroMessage messageCreatingTheSequenceToBeRemoved = createOpenSequence(contextIdForRemoval);
        final byte[] bufferedMessageToBeRemovedValue = getBufferedMessageMessageValue(contextIdForRemoval);

        // give: the contextIdForRemoval has headers
        addBufferedMessagesHeader(SignatureHeaders.SIGNATURE_VALUE_HEADER_KEY, SignatureHeaders.SIGNATURE_VALUE_HEADER_KEY.getBytes(), contextIdForRemoval);
        addBufferedMessagesHeader(SignatureHeaders.SIGNATURE_CERTIFICATE_HEADER_KEY, SignatureHeaders.SIGNATURE_CERTIFICATE_HEADER_KEY.getBytes(), contextIdForRemoval);
        addBufferedMessagesHeader(SignatureHeaders.SIGNATURE_KEY_HEADER_KEY, SignatureHeaders.SIGNATURE_KEY_HEADER_KEY.getBytes(), contextIdForRemoval);

        // given: one sequence expired (contextIdExpired), the other marked for removal (contextIdForRemoval)
        expireSequence(contextIdExpired);
        markForRemovalInThePast(contextIdForRemoval);

        // when: run housekeeping to delete sequences ready for removal
        housekeepingService.deleteSequencesReadyForRemoval();

        // then: the for removal sequence and its messages are deleted
        assertThat(findSequenceInstanceByContextId(contextIdForRemoval.toString()))
                .isEmpty();
        assertThat(countSequencedMessages(contextIdForRemoval))
                .isZero();
        assertThat(countBufferedMessages(contextIdForRemoval))
                .isZero();

        // then: the expired (but not marked for removal) sequence and its messages are still there
        assertThat(findSequenceInstanceByContextId(contextIdExpired.toString()))
                .isNotEmpty();
        assertThat(countSequencedMessages(contextIdExpired))
                .isOne();
        assertThat(countBufferedMessages(contextIdExpired))
                .isOne();

        // then: assert buffered message of deleted sequence has been sent to the error handling
        MessageProcessingFailedEvent mpfe = messageRecorder.getExactlyOneMessageProcessingFailedEvent();
        assertThat(mpfe.getProcessId()).isEqualTo(contextIdForRemoval.toString());
        MessageProcessingFailedPayload mpfePayload = mpfe.getPayload();
        assertThat(mpfePayload.getOriginalMessage().array()).isEqualTo(bufferedMessageToBeRemovedValue);
        assertThat(mpfePayload.getErrorMessage()).matches("Sequence of type %s with id \\d+ for context %s deleted.".
                formatted("TwoSequentialMessages", contextIdForRemoval));
        FailedMessageMetadata mpfeFailedMessageMetadata = mpfePayload.getFailedMessageMetadata();
        assertThat(mpfeFailedMessageMetadata.getEventId()).isEqualTo(messageCreatingTheSequenceToBeRemoved.getIdentity().getId());
        assertThat(mpfeFailedMessageMetadata.getIdempotenceId()).isEqualTo(messageCreatingTheSequenceToBeRemoved.getIdentity().getIdempotenceId());
        assertThat(mpfeFailedMessageMetadata.getMessageTypeName()).isEqualTo(messageCreatingTheSequenceToBeRemoved.getType().getName());
        assertThat(mpfeFailedMessageMetadata.getService()).isEqualTo(messageCreatingTheSequenceToBeRemoved.getPublisher().getService());
        assertThat(mpfeFailedMessageMetadata.getSystem()).isEqualTo(messageCreatingTheSequenceToBeRemoved.getPublisher().getSystem());
        Map<String, ByteBuffer> headers = mpfeFailedMessageMetadata.getHeaders();
        assertThat(headers.get(SignatureHeaders.SIGNATURE_VALUE_HEADER_KEY).array()).
                isEqualTo(SignatureHeaders.SIGNATURE_VALUE_HEADER_KEY.getBytes());
        assertThat(headers.get(SignatureHeaders.SIGNATURE_CERTIFICATE_HEADER_KEY).array()).
                isEqualTo(SignatureHeaders.SIGNATURE_CERTIFICATE_HEADER_KEY.getBytes());
        assertThat(headers.get(SignatureHeaders.SIGNATURE_KEY_HEADER_KEY).array()).
                isEqualTo(SignatureHeaders.SIGNATURE_KEY_HEADER_KEY.getBytes());
    }

    @Test
    void testHousekeeping_deleteClosedSequences() {
        // given: a test event with a predecessor that is buffered and waiting
        UUID contextIdClosed = randomContextId();
        UUID contextIdOpen = randomContextId();
        createClosedSequence(contextIdClosed);
        createOpenSequence(contextIdOpen);

        // when: run housekeeping
        housekeepingService.deleteClosedSequenceInstances();

        // then: the closed sequence and its messages are deleted
        assertThat(findSequenceInstanceByContextId(contextIdClosed.toString()))
                .isEmpty();
        assertThat(countSequencedMessages(contextIdClosed))
                .isZero();
        assertThat(countBufferedMessages(contextIdClosed))
                .isZero();

        // then: the open sequence and its messages are still there
        assertThat(findSequenceInstanceByContextId(contextIdOpen.toString()))
                .isNotEmpty();
        assertThat(countSequencedMessages(contextIdOpen))
                .isOne();
        assertThat(countBufferedMessages(contextIdOpen))
                .isOne();
    }

    @Test
    void testHousekeeping_markExpiredSequencesForDelayedRemoval() {
        // given: open sequences, one expired and one not expired
        UUID contextIdExpired = randomContextId();
        UUID contextIdNotExpired = randomContextId();
        createOpenSequence(contextIdExpired);
        createOpenSequence(contextIdNotExpired);

        // given: expire the open sequence
        expireSequence(contextIdExpired);

        // when: run housekeeping
        housekeepingService.markExpiredSequencesForDelayedRemoval();

        // then: the expired sequence is marked for delayed removal
        assertThat(isMarkedForDelayedRemoval(contextIdExpired))
                .isTrue();

        // then: the not expired sequence is not marked for delayed removal
        assertThat(isMarkedForDelayedRemoval(contextIdNotExpired))
                .isFalse();

        // then: both sequences and their messages are still there
        assertThat(findSequenceInstanceByContextId(contextIdExpired.toString()))
                .isNotEmpty();
        assertThat(findSequenceInstanceByContextId(contextIdNotExpired.toString()))
                .isNotEmpty();
        assertThat(countSequencedMessages(contextIdExpired))
                .isOne();
        assertThat(countSequencedMessages(contextIdNotExpired))
                .isOne();
    }

    private int countBufferedMessages(UUID contextId) {
        return jdbcTemplate.queryForObject("""
                        SELECT count(*) FROM buffered_message WHERE sequence_instance_id =
                        (SELECT id FROM sequence_instance WHERE context_id = ?)
                        """, Integer.class,
                contextId.toString());
    }
    private byte[] getBufferedMessageMessageValue(UUID contextId) {
        return jdbcTemplate.queryForObject("""
                        SELECT message_value FROM buffered_message WHERE sequence_instance_id =
                        (SELECT id FROM sequence_instance WHERE context_id = ?)
                        """, byte[].class,
                contextId.toString());
    }

    private void addBufferedMessagesHeader(String headerName, byte[] headerValue, UUID contextId) {
        jdbcTemplate.update("""
                INSERT INTO message_header (id, header_name, header_value, buffered_message_id)
                SELECT nextval('message_header_sequence'), ?, ?, bm.id
                FROM buffered_message bm
                WHERE bm.sequence_instance_id = (SELECT id FROM sequence_instance WHERE context_id = ?)
                """, headerName, headerValue, contextId.toString());
    }

    private int countSequencedMessages(UUID contextId) {
        return jdbcTemplate.queryForObject("""
                        SELECT count(*) FROM buffered_message WHERE sequence_instance_id =
                        (SELECT id FROM sequence_instance WHERE context_id = ?)
                        """, Integer.class,
                contextId.toString());
    }

    private boolean isMarkedForDelayedRemoval(UUID contextId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM sequence_instance WHERE context_id = ? AND remove_after IS NOT NULL",
                Integer.class,
                contextId.toString());
        return count != null && count > 0;
    }

    private void expireSequence(UUID contextIdExpired) {
        jdbcTemplate.update("UPDATE sequence_instance SET retain_until = NOW() - INTERVAL '1 year' WHERE context_id = ?", contextIdExpired.toString());
    }

    private void markForRemovalInThePast(UUID contextIdExpired) {
        jdbcTemplate.update("UPDATE sequence_instance SET remove_after = NOW() - INTERVAL '1 year' WHERE context_id = ?", contextIdExpired.toString());
    }

    private void createClosedSequence(UUID contextId) {
        JmeDeclarationCreatedEvent predecessorEvent = createDeclarationCreatedEvent(contextId);
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, predecessorEvent);
        JmeSimpleTestEvent successorEvent = createJmeSimpleTestEvent(contextId);
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, successorEvent);
        assertSequenceClosed(contextId);
    }

    private AvroMessage createOpenSequence(UUID contextId) {
        JmeSimpleTestEvent event = createJmeSimpleTestEvent(contextId);
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, event);
        assertMessageStateWaitingAndBuffered(event);
        assertSequenceOpen(event);
        return event;
    }

}
