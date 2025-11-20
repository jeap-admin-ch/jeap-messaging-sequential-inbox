package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.sequentialinbox.actions.SequentialInboxPendingActionsService;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstancePendingAction;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessagePendingAction;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.Optional;
import java.util.UUID;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.createJmeSimpleTestEvent;
import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.randomContextId;

@Slf4j
@TestPropertySource(properties = "jeap.messaging.kafka.expose-message-key-to-consumer=true")
@ContextConfiguration(classes = {SequentialInboxPendingActionsIT.NoOpLockProviderConfig.class})
class SequentialInboxPendingActionsIT extends SequentialInboxITBase {

    @Autowired
    private SequentialInboxPendingActionsService sequentialInboxPendingActionsService;

    @Test
    void testInbox_messageWithPredecessor_waitingAndBuffered_pendingActionConsume() {
        // given: a test event with a predecessor
        UUID contextId = randomContextId();
        JmeSimpleTestEvent event = createJmeSimpleTestEvent(contextId);

        // when: sending the event
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, event);

        // then: assert that the event was buffered and not consumed by the message listener
        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(event);
        assertMessageStateWaitingAndBuffered(event);
        assertSequenceOpen(event);
        assertBufferedMessageCount(contextId, 1);

        updateSequencedMessagePendingAction(SequencedMessagePendingAction.CONSUME, event.getIdentity().getIdempotenceId());

        sequentialInboxPendingActionsService.runPendingActionsOnMessages();

        assertMessageCountHandledByInbox(1);
        assertMessageConsumedByListener(event);
    }

    @Test
    void testInbox_messageWithPredecessor_waitingAndBuffered_pendingActionExpire() {
        // given: a test event with a predecessor
        UUID contextId = randomContextId();
        JmeSimpleTestEvent event = createJmeSimpleTestEvent(contextId);

        // when: sending the event
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, event);

        // then: assert that the event was buffered and not consumed by the message listener
        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(event);
        assertMessageStateWaitingAndBuffered(event);
        assertSequenceOpen(event);
        assertBufferedMessageCount(contextId, 1);

        updateSequencedMessagePendingAction(SequencedMessagePendingAction.EXPIRE, event.getIdentity().getIdempotenceId());

        sequentialInboxPendingActionsService.runPendingActionsOnMessages();

        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(event);

    }

    @Test
    void testInbox_sequenceInstanceOpen_pendingActionClose() {
        // given: a test event with a predecessor
        UUID contextId = randomContextId();
        JmeSimpleTestEvent event = createJmeSimpleTestEvent(contextId);

        // when: sending the event
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, event);

        // then: assert that the event was buffered and not consumed by the message listener
        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(event);
        assertMessageStateWaitingAndBuffered(event);
        assertSequenceOpen(event);
        assertBufferedMessageCount(contextId, 1);

        updateSequenceInstancePendingAction(SequenceInstancePendingAction.CLOSE, contextId.toString());

        sequentialInboxPendingActionsService.runPendingActionsOnSequences();
        assertMessageNotConsumedByListener(event);
        assertSequenceClosed(contextId);
    }

    @Test
    void testInbox_sequenceInstanceOpen_pendingActionConsumeAll() {
        // given: a test event with a predecessor
        UUID contextId = randomContextId();
        JmeSimpleTestEvent event = createJmeSimpleTestEvent(contextId);

        // when: sending the event
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, event);

        // then: assert that the event was buffered and not consumed by the message listener
        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(event);
        assertMessageStateWaitingAndBuffered(event);
        assertSequenceOpen(event);
        assertBufferedMessageCount(contextId, 1);

        updateSequenceInstancePendingAction(SequenceInstancePendingAction.CONSUME_ALL, contextId.toString());

        sequentialInboxPendingActionsService.runPendingActionsOnSequences();

        assertSequenceOpen(contextId);
        assertMessageCountHandledByInbox(1);
        assertMessageConsumedByListener(event);
    }

    private void updateSequencedMessagePendingAction(SequencedMessagePendingAction pendingAction, String idempotenceId) {
        jdbcTemplate.update("UPDATE sequenced_message set pending_action = ? WHERE idempotence_id = ?", pendingAction.name(), idempotenceId);
    }

    private void updateSequenceInstancePendingAction(SequenceInstancePendingAction pendingAction, String contextId) {
        jdbcTemplate.update("UPDATE sequence_instance set pending_action = ? WHERE context_id = ?", pendingAction.name(), contextId);
    }


     @TestConfiguration
     static class NoOpLockProviderConfig {
        @Bean
        public LockProvider lockProvider() {
            return lockConfig -> Optional.of(() -> {});
        }
    }
}
