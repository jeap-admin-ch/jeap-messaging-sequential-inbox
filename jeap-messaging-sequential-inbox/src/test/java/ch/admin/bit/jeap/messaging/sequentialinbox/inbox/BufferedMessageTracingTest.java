package ch.admin.bit.jeap.messaging.sequentialinbox.inbox;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextScope;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextUpdater;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequentialInboxTraceContext;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class BufferedMessageTracingTest {

    private static final String TRACE_ID_STRING = "00000000000000010000000000000002";

    private static final class RecordingTraceContextUpdater extends TraceContextUpdater {
        TraceContext lastActivated;
        int callCount = 0;
        int closeCount = 0;
        RecordingTraceContextUpdater() {
            super(null);
        }
        @Override
        public TraceContextScope setTraceContext(TraceContext traceContext) {
            lastActivated = traceContext;
            callCount++;
            return () -> closeCount++;
        }
    }

    @Test
    void updateCurrentTraceContextSkipsActivationWhenContextIsNull() {
        RecordingTraceContextUpdater updater = new RecordingTraceContextUpdater();
        BufferedMessageTracing tracing = new BufferedMessageTracing(Optional.of(updater));

        TraceContextScope traceContextScope = tracing.updateCurrentTraceContext(null);
        try (traceContextScope) {
            // Scope is used for resource management
        }

        assertThat(updater.callCount)
                .as("A null persisted context means no span was active at capture time; activating anything would " +
                        "overwrite the consumer-record-derived span the framework already established.")
                .isZero();
    }

    @Test
    void updateCurrentTraceContextActivatesContextWhenSeqInboxContextFieldsArePopulated() {
        RecordingTraceContextUpdater updater = new RecordingTraceContextUpdater();
        BufferedMessageTracing tracing = new BufferedMessageTracing(Optional.of(updater));
        SequentialInboxTraceContext populated = SequentialInboxTraceContext.builder()
                .traceIdHigh(1L)
                .traceId(2L)
                .spanId(3L)
                .parentSpanId(4L)
                .traceIdString(TRACE_ID_STRING)
                .sampled(Boolean.FALSE)
                .build();

        TraceContextScope traceContextScope = tracing.updateCurrentTraceContext(populated);
        try (traceContextScope) {
            assertUpdaterWasActivated(updater);
            assertTraceContextContent(updater);
        }
    }

    @Test
    void updateCurrentTraceContextReturnedScopeClosesUpdaterScopeSoOuterContextIsRestored() {
        RecordingTraceContextUpdater updater = new RecordingTraceContextUpdater();
        BufferedMessageTracing tracing = new BufferedMessageTracing(Optional.of(updater));
        SequentialInboxTraceContext populated = SequentialInboxTraceContext.builder()
                .traceId(2L)
                .spanId(3L)
                .build();

        TraceContextScope traceContextScope = tracing.updateCurrentTraceContext(populated);
        assertThat(updater.closeCount)
                .as("Scope must remain open until the caller closes it; auto-close would prematurely pop the OTel context.")
                .isZero();

        traceContextScope.close();

        assertThat(updater.closeCount)
                .as("Closing the scope returned by BufferedMessageTracing must close the underlying TraceContextUpdater " +
                        "scope. Otherwise the inner replay context leaks onto the predecessor's thread after the " +
                        "buffered-message processing loop finishes (see TraceContextUpdater Javadoc).")
                .isOne();
    }

    @Test
    void updateCurrentTraceContextSkipsActivationWhenUpdaterBeanIsAbsent() {
        BufferedMessageTracing tracing = new BufferedMessageTracing(Optional.empty());
        SequentialInboxTraceContext populated = SequentialInboxTraceContext.builder()
                .traceId(2L)
                .spanId(3L)
                .build();

        try (TraceContextScope resultScope = tracing.updateCurrentTraceContext(populated)) {
            assertThat(resultScope).isNotNull();
        }
    }

    private void assertUpdaterWasActivated(RecordingTraceContextUpdater updater) {
        assertThat(updater.callCount).isOne();
    }

    private void assertTraceContextContent(RecordingTraceContextUpdater updater) {
        assertThat(updater.lastActivated.getTraceIdHigh()).isEqualTo(1L);
        assertThat(updater.lastActivated.getTraceId()).isEqualTo(2L);
        assertThat(updater.lastActivated.getSpanId()).isEqualTo(3L);
        assertThat(updater.lastActivated.getParentSpanId()).isEqualTo(4L);
        assertThat(updater.lastActivated.getTraceIdString()).isEqualTo(TRACE_ID_STRING);
        assertThat(updater.lastActivated.getSampled()).isFalse();
    }
}
