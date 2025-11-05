package ch.admin.bit.jeap.messaging.sequentialinbox.inbox;

import ch.admin.bit.jeap.messaging.avro.errorevent.MessageHandlerExceptionInformation;
import ch.admin.bit.jeap.messaging.kafka.errorhandling.ErrorServiceSender;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ErrorHandlingService {

    private final ErrorServiceSender errorServiceSender;
    private final MessageRepository messageRepository;
    private final BufferedMessageTracing bufferedMessageTracing;
    private final SequentialInboxDeserializer inboxDeserializer;

    public void sendDeletedSequencedMessageToErrorHandler(SequenceInstance sequenceInstance,
                                                          SequencedMessage sequencedMessage,
                                                          BufferedMessage bufferedMessage) {
        log.debug("Sending deleted sequenced message with id {} from sequence instance id {} to error handler",
                sequencedMessage.getId(), sequenceInstance.getId());
        try (BufferedMessageTracing.TraceContextRestorer ignored =
                     bufferedMessageTracing.updateCurrentTraceContext(sequencedMessage.getTraceContext())) {
            Map<String, byte[]> headers = messageRepository.getHeaders(sequencedMessage);
            Optional<DeserializedMessage> deserializedMessageOpt = getDeserializedMessage(sequencedMessage, bufferedMessage);
            deserializedMessageOpt.ifPresent(d ->
                    log.debug("Message with id {} from sequence instance id {} was deserializable.",
                        sequencedMessage.getId(), sequenceInstance.getId()));
            FailedConsumerRecord failedConsumerRecord = deserializedMessageOpt.map(deserializedMessage ->
                        FailedConsumerRecord.of(sequencedMessage, headers, deserializedMessage.key(), deserializedMessage.message()))
                    .orElseGet(() ->
                        FailedConsumerRecord.of(sequencedMessage, headers, bufferedMessage));
            errorServiceSender.accept(failedConsumerRecord, createMessageProcessingFailedException(sequenceInstance));
            log.debug("Sent deleted sequenced message with id {} from sequence instance id {} to error handler",
                    sequencedMessage.getId(), sequenceInstance.getId());
        }
    }

    private Optional<DeserializedMessage> getDeserializedMessage(SequencedMessage sequencedMessage, BufferedMessage bufferedMessage) {
        try {
            return Optional.of(inboxDeserializer.deserialize(sequencedMessage, bufferedMessage));
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    private Exception createMessageProcessingFailedException(SequenceInstance sequenceInstance) {
        InboxSequenceDeletedException isde = new InboxSequenceDeletedException(sequenceInstance);
        return new ListenerExecutionFailedException(isde.getMessage(), isde);
    }

    static class InboxSequenceDeletedException extends RuntimeException implements MessageHandlerExceptionInformation {

        private final String message;

        InboxSequenceDeletedException(SequenceInstance sequenceInstance) {
            this.message = "Sequence of type %s with id %s for context %s deleted.".formatted(
                    sequenceInstance.getName(), sequenceInstance.getId(), sequenceInstance.getContextId());
        }

        @Override
        public String getErrorCode() {
            return "Sequential-Inbox-Sequence-Deleted";
        }

        @Override
        public @Nullable String getDescription() {
            return "The sequence instance to which this message belonged has expired and has been deleted from the inbox.";
        }

        @Override
        public Temporality getTemporality() {
            return Temporality.PERMANENT;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public @Nullable String getStackTraceAsString() {
            return null;
        }

    }

}

