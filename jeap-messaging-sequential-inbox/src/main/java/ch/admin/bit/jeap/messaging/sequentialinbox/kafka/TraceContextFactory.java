package ch.admin.bit.jeap.messaging.sequentialinbox.kafka;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextProvider;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequentialInboxTraceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;


@Component
@RequiredArgsConstructor
@Slf4j
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class TraceContextFactory {

    private final Optional<TraceContextProvider> traceContextProvider;

    public SequentialInboxTraceContext currentTraceContext() {
        if (traceContextProvider.isEmpty()) {
            return null;
        }

        return createCurrentTraceContext(traceContextProvider.get());
    }

    private static SequentialInboxTraceContext createCurrentTraceContext(TraceContextProvider traceContextProvider) {
        TraceContext traceContext = traceContextProvider.getTraceContext();
        if (traceContext == null) {
            return null;
        }

        return SequentialInboxTraceContext.builder()
                .traceIdHigh(traceContext.getTraceIdHigh())
                .traceId(traceContext.getTraceId())
                .spanId(traceContext.getSpanId())
                .parentSpanId(traceContext.getParentSpanId())
                .traceIdString(traceContext.getTraceIdString())
                .sampled(traceContext.getSampled())
                .build();
    }
}
