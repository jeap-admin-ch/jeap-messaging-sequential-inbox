package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message;

import ch.admin.bit.jeap.domainevent.avro.AvroDomainEventBuilder;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import ch.admin.bit.jme.test.JmeSimpleTestEventPayload;
import ch.admin.bit.jme.test.JmeSimpleTestEventReferences;
import lombok.AccessLevel;
import lombok.Getter;

@Getter
public class JmeSimpleTestEventBuilder extends AvroDomainEventBuilder<JmeSimpleTestEventBuilder, JmeSimpleTestEvent> {
    @Getter(AccessLevel.NONE)
    private static final String SPECIFIED_MESSAGE_TYPE_VERSION = "1.0.0";
    private String serviceName = "test";
    @Getter(AccessLevel.NONE)
    private static final String SYSTEM_NAME = "test";
    private String message;
    private String processId;

    private JmeSimpleTestEventBuilder() {
        super(JmeSimpleTestEvent::new);
    }

    public static JmeSimpleTestEventBuilder create() {
        return new JmeSimpleTestEventBuilder();
    }

    public JmeSimpleTestEventBuilder message(String message) {
        this.message = message;
        return self();
    }

    public JmeSimpleTestEventBuilder serviceName(String serviceName) {
        this.serviceName = serviceName;
        return self();
    }

    public JmeSimpleTestEventBuilder processId(String processId) {
        this.processId = processId;
        return self();
    }

    @Override
    protected JmeSimpleTestEventBuilder self() {
        return this;
    }

    @Override
    public JmeSimpleTestEvent build() {
        setReferences(JmeSimpleTestEventReferences.newBuilder().build());
        setPayload(JmeSimpleTestEventPayload.newBuilder().setMessage(message).build());
        setProcessId(processId);
        return super.build();
    }

    @Override
    public String getSpecifiedMessageTypeVersion() {
        return SPECIFIED_MESSAGE_TYPE_VERSION;
    }

    @Override
    public String getSystemName() {
        return SYSTEM_NAME;
    }
}
