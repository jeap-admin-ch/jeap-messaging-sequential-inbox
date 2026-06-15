package ch.admin.bit.jeap.messaging.sequentialinbox.spring;

import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class SequentialInboxMessageHandlerTest {

    private SequentialInboxMessageHandler sequentialInboxMessageHandler;
    @Mock
    private AvroMessageKey avroMessageKey;
    @Mock
    private AvroMessage avroMessage;

    private boolean targetMethodInvoked = false;
    private boolean targetMethodWithKeyInvoked = false;

    private class InvocationTestTarget {

        public void targetMethod(AvroMessage avroMessage) {
            Objects.requireNonNull(avroMessage);
            targetMethodInvoked = true;
        }

        public void targetMethodThrowingException(AvroMessage avroMessage) {
            Objects.requireNonNull(avroMessage);
            throw new IllegalStateException("cause");
        }

        public void targetMethodThrowingCheckedException(AvroMessage avroMessage) throws IOException {
            Objects.requireNonNull(avroMessage);
            throw new IOException("checked exception cause");
        }

        public void targetMethodWithKey(AvroMessageKey avroMessageKey, AvroMessage avroMessage) {
            Objects.requireNonNull(avroMessageKey);
            Objects.requireNonNull(avroMessage);
            targetMethodWithKeyInvoked = true;
        }
    }

    private static class DifferentClass {
        public void someMethod() {
            // This is just a test class, no implementation needed
        }
    }

    @Test
    void invokeWithValidParameters() {
        Object bean = new InvocationTestTarget();
        Method method = getMethod(bean.getClass(), "targetMethod", AvroMessage.class);
        ListenerBeanMethod listenerBeanMethod = new ListenerBeanMethod(method, bean, AvroMessage.class);

        sequentialInboxMessageHandler = new SequentialInboxMessageHandler(listenerBeanMethod, false);
        sequentialInboxMessageHandler.invoke(null, avroMessage);

        assertThat(targetMethodInvoked).isTrue();
    }

    @Test
    void invokeWithValidParametersWithKey() {
        Object bean = new InvocationTestTarget();
        Method method = getMethod(bean.getClass(), "targetMethodWithKey", AvroMessageKey.class, AvroMessage.class);
        ListenerBeanMethod listenerBeanMethod = new ListenerBeanMethod(method, bean, AvroMessage.class);

        sequentialInboxMessageHandler = new SequentialInboxMessageHandler(listenerBeanMethod, true);
        sequentialInboxMessageHandler.invoke(avroMessageKey, avroMessage);

        assertThat(targetMethodWithKeyInvoked).isTrue();
    }

    @Test
    void invokeThrows() {
        Object bean = new InvocationTestTarget();
        Method method = getMethod(bean.getClass(), "targetMethodThrowingException", AvroMessage.class);
        ListenerBeanMethod listenerBeanMethod = new ListenerBeanMethod(method, bean, AvroMessage.class);

        sequentialInboxMessageHandler = new SequentialInboxMessageHandler(listenerBeanMethod, false);

        assertThatThrownBy(() -> sequentialInboxMessageHandler.invoke(avroMessageKey, avroMessage))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cause");
    }

    @Test
    void invokeThrowsCheckedException() {
        Object bean = new InvocationTestTarget();
        Method method = getMethod(bean.getClass(), "targetMethodThrowingCheckedException", AvroMessage.class);
        ListenerBeanMethod listenerBeanMethod = new ListenerBeanMethod(method, bean, AvroMessage.class);

        sequentialInboxMessageHandler = new SequentialInboxMessageHandler(listenerBeanMethod, false);

        assertThatThrownBy(() -> sequentialInboxMessageHandler.invoke(avroMessageKey, avroMessage))
                .isInstanceOf(SequentialInboxException.class)
                .hasMessageContaining("Invocation of message handler method");
    }

    @Test
    void invokeReflectionError() {
        Object bean = new InvocationTestTarget();
        Method differentObjectMethod = getMethod(DifferentClass.class, "someMethod");
        ListenerBeanMethod listenerBeanMethod = new ListenerBeanMethod(differentObjectMethod, bean, AvroMessage.class);

        sequentialInboxMessageHandler = new SequentialInboxMessageHandler(listenerBeanMethod, false);

        assertThatThrownBy(() -> sequentialInboxMessageHandler.invoke(avroMessageKey, avroMessage))
                .isInstanceOf(SequentialInboxException.class)
                .hasMessageContaining("Reflection error");
    }

    @Test
    void toStringContainsMethodAndKeyFlag() {
        Object bean = new Object();
        Method method = getMethod(bean.getClass(), "equals", Object.class);
        SequentialInboxMessageHandler messageHandler =
                new SequentialInboxMessageHandler(new ListenerBeanMethod(method, bean, AvroMessage.class), false);

        assertThat(messageHandler.toString())
                .hasToString("java.lang.Object#equals key=false");
    }

    private Method getMethod(Class<?> type, String methodName, Class<?>... parameterTypes) {
        try {
            return type.getMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException ex) {
            throw new AssertionError(ex);
        }
    }
}
