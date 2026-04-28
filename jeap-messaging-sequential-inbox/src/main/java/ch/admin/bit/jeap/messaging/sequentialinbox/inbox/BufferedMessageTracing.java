package ch.admin.bit.jeap.messaging.sequentialinbox.inbox;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextScope;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextUpdater;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequentialInboxTraceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class BufferedMessageTracing {

    private static final TraceContextScope NOOP = () -> { };

    private final Optional<TraceContextUpdater> traceContextUpdaterBean;

    TraceContextScope updateCurrentTraceContext(SequentialInboxTraceContext inboxTraceContext) {
        if (inboxTraceContext == null || traceContextUpdaterBean.isEmpty()) {
            return NOOP;
        }
        log.debug("Original trace context found on buffered message (traceId={}). Overriding the current tracing context with the tracing context {}.",
                inboxTraceContext.getTraceIdString(), inboxTraceContext);
        return traceContextUpdaterBean.get().setTraceContext(toTraceContext(inboxTraceContext));
    }

    private static TraceContext toTraceContext(SequentialInboxTraceContext inboxTraceContext) {
        return new TraceContext(inboxTraceContext.getTraceIdHigh(), inboxTraceContext.getTraceId(),
                inboxTraceContext.getSpanId(), inboxTraceContext.getParentSpanId(),
                inboxTraceContext.getTraceIdString(), inboxTraceContext.getSampled());
    }
}
