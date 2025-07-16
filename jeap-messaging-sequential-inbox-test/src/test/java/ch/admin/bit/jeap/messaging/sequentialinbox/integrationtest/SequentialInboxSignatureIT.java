package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.avro.errorevent.MessageProcessingFailedEvent;
import ch.admin.bit.jeap.messaging.kafka.contract.ContractsValidator;
import ch.admin.bit.jeap.messaging.kafka.signature.SignatureHeaders;
import ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.DeclarationCreatedEventListener;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
import ch.admin.bit.jme.declaration.JmeDeclarationCreatedEvent;
import ch.admin.bit.jme.test.BeanReferenceMessageKey;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.*;
import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext
@AutoConfigureObservability
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.application.name=jme-messaging-receiverpublisher-outbox-service"})
@Slf4j
@ActiveProfiles("test-signing")
@TestPropertySource(properties = "jeap.messaging.sequential-inbox.config-location=classpath:/messaging/jeap-sequential-inbox-for-encryption-and-signature.yml")
class SequentialInboxSignatureIT extends SequentialInboxITBase {

    @MockitoBean
    @SuppressWarnings("unused")
    private ContractsValidator contractsValidator; // Disable contract checking by mocking the contracts validator

    @Test
    void signMessage_and_receiveSignedMessage() {
        // given: a test event
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent event = createDeclarationCreatedEvent(contextId,"jme-messaging-receiverpublisher-outbox-service");

        // when: sending the event
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, event);

        // then: assert that the event was consumed by the message listener
        assertSequenceState(contextId.toString(), SequenceInstanceState.OPEN);

        JmeSimpleTestEvent jmeSimpleTestEvent = createJmeSimpleTestEvent(contextId, "jme-messaging-receiverpublisher-outbox-service");
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, jmeSimpleTestEvent);

        assertMessageConsumedByListener(event);
        assertSequencedMessageProcessedSuccessfully(event);
        assertSequenceState(contextId.toString(), SequenceInstanceState.CLOSED);
        assertBufferedMessageCount(contextId, 1);
        assertSequenceClosed(contextId);

    }

    @Test
    void signMessage_exceptionWhileImmediateProcessing_expectSignatureHeadersOnMessageProcessingFailedEvent() {
        // given: a test event
        UUID contextId = randomContextId();
        JmeSimpleTestEvent event = createJmeSimpleTestEvent(contextId, "jme-messaging-receiverpublisher-outbox-service");
        event.getPayload().setMessage(DeclarationCreatedEventListener.FAILURE);
        AvroMessageKey key = createKey();

        // when: sending the event
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, key, event);

        // then: assert that the event was consumed by the message listener
        assertSequenceState(contextId.toString(), SequenceInstanceState.OPEN);

        // then: assert that the message processing failed event contains the signature headers
        MessageProcessingFailedEvent mpfe = messageRecorder.getExactlyOneMessageProcessingFailedEvent();
        Map<String, ByteBuffer> headers = mpfe.getPayload().getFailedMessageMetadata().getHeaders();
        assertThat(headers)
                .containsKey(SignatureHeaders.SIGNATURE_VALUE_HEADER_KEY)
                .containsKey(SignatureHeaders.SIGNATURE_KEY_HEADER_KEY)
                .containsKey(SignatureHeaders.SIGNATURE_CERTIFICATE_HEADER_KEY);
    }

    @Test
    void signMessage_exceptionWhileDeferredProcessing_expectSignatureHeadersOnMessageProcessingFailedEvent() {
        // given: a buffered test event that will fail during deferred processing
        UUID contextId = randomContextId();
        JmeDeclarationCreatedEvent bufferedEvent = createDeclarationCreatedEvent(contextId,"jme-messaging-receiverpublisher-outbox-service");
        bufferedEvent.getPayload().setMessage(DeclarationCreatedEventListener.FAILURE);
        AvroMessageKey key = createKey();
        // when: sending the event
        sendSync(JmeDeclarationCreatedEvent.TypeRef.DEFAULT_TOPIC, bufferedEvent);
        // then: assert that the event was consumed by the message listener
        assertSequenceState(contextId.toString(), SequenceInstanceState.OPEN);

        // given: a predecessor event that will trigger the deferred processing
        JmeSimpleTestEvent predecessorEvent = createJmeSimpleTestEvent(contextId, "jme-messaging-receiverpublisher-outbox-service");
        // when: sending the event
        sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, key, predecessorEvent);

        // then: assert that the message processing failed event contains the signature headers
        MessageProcessingFailedEvent mpfe = messageRecorder.getExactlyOneMessageProcessingFailedEvent();
        Map<String, ByteBuffer> headers = mpfe.getPayload().getFailedMessageMetadata().getHeaders();
        assertThat(headers)
                .containsKey(SignatureHeaders.SIGNATURE_VALUE_HEADER_KEY)
                .containsKey(SignatureHeaders.SIGNATURE_KEY_HEADER_KEY)
                .containsKey(SignatureHeaders.SIGNATURE_CERTIFICATE_HEADER_KEY);
    }

    private static AvroMessageKey createKey() {
        return BeanReferenceMessageKey.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setName("test")
                .setNamespace("test").build();
    }
}
