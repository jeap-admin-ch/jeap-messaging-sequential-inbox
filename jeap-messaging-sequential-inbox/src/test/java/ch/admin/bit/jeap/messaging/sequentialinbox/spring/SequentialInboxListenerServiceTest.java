package ch.admin.bit.jeap.messaging.sequentialinbox.spring;

import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SequencedMessageType;
import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SequentialInboxConfiguration;
import ch.admin.bit.jeap.messaging.sequentialinbox.kafka.KafkaSequentialInboxMessageConsumerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SequentialInboxListenerServiceTest {

    @Mock
    private KafkaSequentialInboxMessageConsumerFactory messageConsumerFactory;
    @Mock
    private ApplicationContext applicationContext;
    @Mock
    private SequentialInboxConfiguration sequentialInboxConfiguration;
    @Mock
    private MessageHandlerProvider messageHandlerProvider;

    private SequentialInboxListenerService sequentialInboxListenerService;

    @BeforeEach
    void setUp() {
        sequentialInboxListenerService = new SequentialInboxListenerService(messageConsumerFactory, applicationContext, sequentialInboxConfiguration, messageHandlerProvider);
    }

    @Test
    void startMessageListenersWithValidMessageType() {
        ValidListener bean = new ValidListener();
        mockListener(bean);

        sequentialInboxListenerService.startMessageListeners();

        verify(messageConsumerFactory, times(1))
                .startConsumer(eq("topic"), eq("AvroMessage"), eq("clusterName"), any());
    }

    @Test
    void startMessageListenersWithValidMessageTypeForListenerWithKey() {
        ValidListenerWithKey bean = new ValidListenerWithKey();
        mockListener(bean);

        sequentialInboxListenerService.startMessageListeners();

        verify(messageConsumerFactory, times(1))
                .startConsumer(eq("topic"), eq("AvroMessage"), eq("clusterName"), any());
    }

    @Test
    void startMessageListenersNoListenerForConfiguredSequencedMessageType() {
        Object noListenerBean = new Object();
        mockListener(noListenerBean);

        assertThatExceptionOfType(SequentialInboxException.class)
                .isThrownBy(() -> sequentialInboxListenerService.startMessageListeners())
                .withMessageContaining("No message handler found for message type AvroMessage");
    }

    @Test
    void startMessageListenersInvalidListenerMethodSignature() {
        InvalidListener bean = new InvalidListener();
        mockListener(bean);

        assertThatExceptionOfType(SequentialInboxException.class)
                .isThrownBy(() -> sequentialInboxListenerService.startMessageListeners())
                .withMessageContaining("invalid signature");
    }

    @Test
    void startMessageListenersInvalidListenerMethodSignatureArgs() {
        InvalidListenerArgs bean = new InvalidListenerArgs();
        mockListener(bean);

        assertThatExceptionOfType(SequentialInboxException.class)
                .isThrownBy(() -> sequentialInboxListenerService.startMessageListeners())
                .withMessageContaining("invalid signature");
    }

    @Test
    void startMessageListenersInvalidListenerMethodSignatureNoArgs() {
        InvalidListenerNoArgs bean = new InvalidListenerNoArgs();
        mockListener(bean);

        assertThatExceptionOfType(SequentialInboxException.class)
                .isThrownBy(() -> sequentialInboxListenerService.startMessageListeners())
                .withMessageContaining("invalid signature");
    }

    @Test
    void startMessageListenersInvalidListenerMethodSignatureExtraArgs() {
        InvalidListenerExtraArgs bean = new InvalidListenerExtraArgs();
        mockListener(bean);

        assertThatExceptionOfType(SequentialInboxException.class)
                .isThrownBy(() -> sequentialInboxListenerService.startMessageListeners())
                .withMessageContaining("invalid signature");
    }

    private void mockListener(Object bean) {
        SequencedMessageType messageType = SequencedMessageType.builder()
                .type("AvroMessage").topic("topic").clusterName("clusterName").build();
        when(sequentialInboxConfiguration.getSequencedMessageTypes()).thenReturn(Set.of(messageType));
        when(applicationContext.getBeanDefinitionNames()).thenReturn(new String[]{"beanName"});
        when(applicationContext.getBean("beanName")).thenReturn(bean);
    }

    private static class ValidListener {
        @SequentialInboxMessageListener
        public void listener(AvroMessage message) {
            Objects.requireNonNull(message);
        }
    }

    private static class ValidListenerWithKey {
        @SequentialInboxMessageListener
        public void listener(AvroMessageKey key, AvroMessage message) {
            Objects.requireNonNull(key);
            Objects.requireNonNull(message);
        }
    }

    private static class InvalidListener {
        @SequentialInboxMessageListener
        public void listener(Object foo) {
            Objects.requireNonNull(foo);
        }
    }

    private static class InvalidListenerArgs {
        @SequentialInboxMessageListener
        public void listener(AvroMessage message, AvroMessageKey key) {
            Objects.requireNonNull(message);
            Objects.requireNonNull(key);
        }
    }

    private static class InvalidListenerNoArgs {
        @SequentialInboxMessageListener
        public void listener() {
            // This is just a test listener, no implementation needed
        }
    }

    private static class InvalidListenerExtraArgs {
        @SequentialInboxMessageListener
        public void listener(AvroMessageKey key, AvroMessage message, String foo) {
            Objects.requireNonNull(key);
            Objects.requireNonNull(message);
            Objects.requireNonNull(foo);
        }
    }
}
