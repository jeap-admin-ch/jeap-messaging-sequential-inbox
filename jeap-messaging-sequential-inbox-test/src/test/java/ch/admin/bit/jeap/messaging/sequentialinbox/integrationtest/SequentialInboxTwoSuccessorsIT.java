package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.MultipleTestEventListener;
import ch.admin.bit.jme.declaration.JmeDeclarationCreatedEvent;
import ch.admin.bit.jme.test.JmeEnumTestEvent;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.*;

@TestPropertySource(properties = "jeap.messaging.sequential-inbox.config-location=classpath:/messaging/jeap-sequential-inbox-two-successors.yml")
class SequentialInboxTwoSuccessorsIT extends SequentialInboxITBase {

    @Test
    void inboxTwoMessagesWithPredecessorsBufferedAndThenProcessedAfterPredecessorHandledFailOneThenRetry() {
        // given: two events sharing a predecessor
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent predecessor = createDeclarationCreatedEvent(contextId);
        JmeSimpleTestEvent successorOne = createJmeSimpleTestEvent(contextId);
        JmeEnumTestEvent successorTwo = createEnumTestEvent(contextId);

        // when: sending the successor events before the predecessor event
        sendSync(JmeEnumTestEvent.TypeRef.DEFAULT_TOPIC, successorTwo);
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, successorOne);

        // then: assert that both events were buffered and not yet consumed by the message listener
        assertMessageCountHandledByInbox(2);
        assertMessageNotConsumedByListener(successorOne);
        assertMessageStateWaitingAndBuffered(successorOne);
        assertMessageNotConsumedByListener(successorTwo);
        assertMessageStateWaitingAndBuffered(successorTwo);

        // when: sending the predecessor event and provoking a failure for successor one
        MultipleTestEventListener.setFailOnJmeSimpleTestEvent(true);
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, predecessor);

        // then: assert that the predecessor event was consumed by the message listener
        assertMessageCountHandledByInbox(3);
        assertMessageConsumedByListener(predecessor);
        assertSequencedMessageProcessedSuccessfully(predecessor);

        // then: assert that the successor events were handled, one successfully and one failed
        assertMessageConsumedByListener(successorTwo);
        assertSequencedMessageProcessedSuccessfully(successorTwo);
        assertMessageStateFailed(successorOne);
        assertMessageNotConsumedByListener(successorOne);
        assertSequenceOpen(contextId);

        // when: retrying the failed message
        MultipleTestEventListener.setFailOnJmeSimpleTestEvent(false);
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, successorOne);

        // then: assert that the successor event was consumed by the message listener and the sequence is closed
        assertMessageConsumedByListener(successorOne);
        assertSequencedMessageProcessedSuccessfully(successorOne);
        assertSequenceOfMessages(contextId, predecessor, successorTwo, successorOne);
        assertSequenceClosed(contextId);
    }

}
