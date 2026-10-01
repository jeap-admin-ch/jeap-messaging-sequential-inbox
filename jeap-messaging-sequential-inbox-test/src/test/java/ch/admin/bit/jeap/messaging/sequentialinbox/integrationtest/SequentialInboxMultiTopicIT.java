package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jme.declaration.JmeDeclarationCreatedEvent;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.TestApp.JME_SIMPLE_TEST_EVENT_V2_TOPIC;
import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests a message type which is consumed from more than one topic, as needed while migrating a message type
 * from one topic to another one.
 */
@TestPropertySource(properties = "jeap.messaging.sequential-inbox.config-location=classpath:/messaging/jeap-sequential-inbox-multi-topic.yml")
class SequentialInboxMultiTopicIT extends SequentialInboxITBase {

    @Test
    void messageFromOldTopicIsSequenced() {
        // given: an event with a predecessor
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent predecessor = createDeclarationCreatedEvent(contextId);
        JmeSimpleTestEvent successor = createJmeSimpleTestEvent(contextId);

        // when: sending the successor to the old topic of the message type
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, successor);

        // then: assert that the event was buffered and not yet consumed by the message listener
        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(successor);
        assertMessageStateWaitingAndBuffered(successor);

        // when: sending the predecessor event for the same context ID
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, predecessor);

        // then: assert that both events were consumed in the declared order
        assertMessageConsumedByListener(predecessor, successor);
        assertSequencedMessageProcessedSuccessfully(predecessor, successor);
        assertSequenceOfMessages(contextId, predecessor, successor);
        assertSequenceClosed(contextId);
    }

    @Test
    void messageFromNewTopicIsSequenced() {
        // given: an event with a predecessor
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent predecessor = createDeclarationCreatedEvent(contextId);
        JmeSimpleTestEvent successor = createJmeSimpleTestEvent(contextId);

        // when: sending the successor to the new topic of the message type
        sendSync(JME_SIMPLE_TEST_EVENT_V2_TOPIC, successor);

        // then: assert that the event was buffered and not yet consumed by the message listener
        assertMessageCountHandledByInbox(1);
        assertMessageNotConsumedByListener(successor);
        assertMessageStateWaitingAndBuffered(successor);

        // when: sending the predecessor event for the same context ID
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, predecessor);

        // then: assert that both events were consumed in the declared order
        assertMessageConsumedByListener(predecessor, successor);
        assertSequencedMessageProcessedSuccessfully(predecessor, successor);
        assertSequenceOfMessages(contextId, predecessor, successor);
        assertSequenceClosed(contextId);

        // then: assert that the topic the message has been received from is recorded for the sequenced message
        assertTopicRecordedForMessage(successor, JME_SIMPLE_TEST_EVENT_V2_TOPIC);
    }

    @Test
    void messageReceivedFromBothTopicsIsProcessedOnlyOnce() {
        // given: an event which has already been processed, and its predecessor
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent predecessor = createDeclarationCreatedEvent(contextId);
        JmeSimpleTestEvent successor = createJmeSimpleTestEvent(contextId);
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, predecessor);
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, successor);
        assertMessageConsumedByListener(predecessor, successor);

        // when: the very same event is received a second time from the new topic, i.e. because it has been
        // copied to the new topic during the topic migration
        sendSync(JME_SIMPLE_TEST_EVENT_V2_TOPIC, successor);

        // then: assert that the duplicate has been handled by the inbox, but not a second time by the listener
        assertMessageCountHandledByInbox(3);
        await("Duplicated message is consumed by the listener only once")
                .untilAsserted(() -> assertThat(messageRecorder.countConsumedMessagesForContext(contextId))
                        .isEqualTo(2));
        assertSequencedMessageCount(contextId, 2);
        assertSequenceClosed(contextId);
    }

    private void assertTopicRecordedForMessage(JmeSimpleTestEvent message, String expectedTopic) {
        String topic = jdbcTemplate.queryForObject(
                "select topic from sequenced_message where message_type = ? and idempotence_id = ?",
                String.class, message.getType().getName(), message.getIdentity().getIdempotenceId());
        assertThat(topic)
                .describedAs("Topic the message has been received from is recorded for the sequenced message")
                .isEqualTo(expectedTopic);
    }
}
