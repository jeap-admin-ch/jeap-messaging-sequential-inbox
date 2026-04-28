package ch.admin.bit.jeap.messaging.sequentialinbox.kafka;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextProvider;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequentialInboxTraceContext;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TraceContextFactoryTest {

    private static final class StubTraceContextProvider extends TraceContextProvider {
        private final TraceContext toReturn;
        StubTraceContextProvider(TraceContext toReturn) {
            super(null);
            this.toReturn = toReturn;
        }
        @Override
        public TraceContext getTraceContext() {
            return toReturn;
        }
    }

    @Test
    void currentTraceContext_propagatesAllFieldsFromProviderToPersistedContext() {
        TraceContextProvider provider = new StubTraceContextProvider(
                new TraceContext(1L, 2L, 3L, 4L, "00000000000000010000000000000002", Boolean.FALSE));
        TraceContextFactory factory = new TraceContextFactory(Optional.of(provider));

        SequentialInboxTraceContext result = factory.currentTraceContext();

        assertThat(result.getTraceIdHigh()).isEqualTo(1L);
        assertThat(result.getTraceId()).isEqualTo(2L);
        assertThat(result.getSpanId()).isEqualTo(3L);
        assertThat(result.getParentSpanId()).isEqualTo(4L);
        assertThat(result.getTraceIdString()).isEqualTo("00000000000000010000000000000002");
        assertThat(result.getSampled()).isFalse();
    }

    @Test
    void currentTraceContext_returnsNull_whenProviderReturnsNull() {
        TraceContextProvider provider = new StubTraceContextProvider(null);
        TraceContextFactory factory = new TraceContextFactory(Optional.of(provider));

        SequentialInboxTraceContext result = factory.currentTraceContext();

        assertThat(result).isNull();
    }

    @Test
    void currentTraceContext_returnsNull_whenNoProviderPresent() {
        TraceContextFactory factory = new TraceContextFactory(Optional.empty());

        SequentialInboxTraceContext result = factory.currentTraceContext();

        assertThat(result).isNull();
    }
}
