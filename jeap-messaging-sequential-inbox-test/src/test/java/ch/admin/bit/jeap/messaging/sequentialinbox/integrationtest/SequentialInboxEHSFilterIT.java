package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jme.declaration.JmeDeclarationCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.createDeclarationCreatedEvent;
import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.randomContextId;

@Slf4j
@TestPropertySource(properties = "jeap.messaging.kafka.expose-message-key-to-consumer=true")
class SequentialInboxEHSFilterIT extends SequentialInboxITBase {

    @Test
    void testInbox_messageWithoutPredecessorButWithOtherService_notProcessed() throws InterruptedException {
        // given: a test event
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent event1 = createDeclarationCreatedEvent(contextId);
        JmeDeclarationCreatedEvent event2 = createDeclarationCreatedEvent(contextId);

        // when: sending the event
        Header headerEHS = new RecordHeader("jeap_eh_error_handling_service", "test-error-handling-service".getBytes());
        Header headerOtherService = new RecordHeader("jeap_eh_target_service", "other-service-1".getBytes());

        sendSyncWithHeaders(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, event1, headerEHS, headerOtherService);
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, event2);

        // then: event2 should be consumed, event1 should have been filtered
        assertMessageConsumedByListener(event2);
        assertMessageNotConsumedByListener(event1);
        assertSequencedMessageProcessedSuccessfully(event2);
        assertBufferedMessageCount(contextId, 0);
    }

}
