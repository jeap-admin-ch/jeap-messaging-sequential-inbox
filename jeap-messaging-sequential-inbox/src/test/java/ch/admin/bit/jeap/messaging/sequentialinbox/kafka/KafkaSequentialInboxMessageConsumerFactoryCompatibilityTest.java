package ch.admin.bit.jeap.messaging.sequentialinbox.kafka;

import ch.admin.bit.jeap.messaging.sequentialinbox.spring.SequentialInboxMessageHandler;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaSequentialInboxMessageConsumerFactoryCompatibilityTest {

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t", "old-topic"})
    void legacySingleTopicApiDelegatesWithDefaultFallback(String topic) {
        var factory = mock(KafkaSequentialInboxMessageConsumerFactory.class, CALLS_REAL_METHODS);
        var handler = mock(SequentialInboxMessageHandler.class);
        List<String> expectedTopics = topic == null || topic.isBlank() ? List.of() : List.of(topic);
        doNothing().when(factory).startConsumer(expectedTopics, "MessageType", "cluster", handler);

        factory.startConsumer(topic, "MessageType", "cluster", handler);

        verify(factory).startConsumer(expectedTopics, "MessageType", "cluster", handler);
    }
}
