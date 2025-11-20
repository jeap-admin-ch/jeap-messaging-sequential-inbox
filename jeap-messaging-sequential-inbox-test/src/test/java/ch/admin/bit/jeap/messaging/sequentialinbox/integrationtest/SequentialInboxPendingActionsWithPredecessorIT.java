package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.sequentialinbox.actions.SequentialInboxPendingActionsService;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessagePendingAction;
import ch.admin.bit.jme.test.JmeEnumTestEvent;
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

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.*;

@Slf4j
@TestPropertySource(properties = "jeap.messaging.sequential-inbox.config-location=classpath:/messaging/jeap-sequential-inbox-for-pending-actions.yml")
@ContextConfiguration(classes = {SequentialInboxPendingActionsWithPredecessorIT.NoOpLockProviderConfig.class})
class SequentialInboxPendingActionsWithPredecessorIT extends SequentialInboxITBase {

    @Autowired
    private SequentialInboxPendingActionsService sequentialInboxPendingActionsService;

    @Test
    void testInbox_messageWithPredecessor_waitingAndBuffered_pendingActionConsume() {
        // given: a test jmeSimpleTestEvent with a predecessor
        UUID contextId = randomContextId();
        JmeSimpleTestEvent jmeSimpleTestEvent = createJmeSimpleTestEvent(contextId);
        JmeEnumTestEvent jmeEnumTestEvent = createEnumTestEvent(contextId);

        // when: sending the jmeSimpleTestEvent and jmeEnumTestEvent
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, jmeSimpleTestEvent);
        sendSync(JmeEnumTestEvent.TypeRef.DEFAULT_TOPIC, jmeEnumTestEvent);

        // then: assert that the jmeSimpleTestEvent and jmeEnumTestEvent were buffered and not consumed by the message listener
        assertMessageCountHandledByInbox(2);
        assertMessageNotConsumedByListener(jmeSimpleTestEvent, jmeEnumTestEvent);
        assertMessageStateWaitingAndBuffered(jmeSimpleTestEvent, jmeEnumTestEvent);
        assertSequenceOpen(jmeSimpleTestEvent, jmeEnumTestEvent);
        assertBufferedMessageCount(contextId, 2);

        // when: updating the pending action to CONSUME for the jmeSimpleTestEvent
        updateSequencedMessagePendingAction(SequencedMessagePendingAction.CONSUME, jmeSimpleTestEvent.getIdentity().getIdempotenceId());

        // and: running the pending actions service
        sequentialInboxPendingActionsService.runPendingActionsOnMessages();

        // then: assert that both events were consumed by the message listener
        assertMessageCountHandledByInbox(2);
        assertMessageConsumedByListener(jmeSimpleTestEvent, jmeEnumTestEvent);
    }

    private void updateSequencedMessagePendingAction(SequencedMessagePendingAction pendingAction, String idempotenceId) {
        jdbcTemplate.update("UPDATE sequenced_message set pending_action = ? WHERE idempotence_id = ?", pendingAction.name(), idempotenceId);
    }


     @TestConfiguration
     static class NoOpLockProviderConfig {
        @Bean
        public LockProvider lockProvider() {
            return lockConfig -> Optional.of(() -> {});
        }
    }
}
