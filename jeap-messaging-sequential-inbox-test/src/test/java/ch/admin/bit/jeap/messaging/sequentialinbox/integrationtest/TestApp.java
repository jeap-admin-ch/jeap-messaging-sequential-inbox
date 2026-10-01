package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContract;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContracts;
import ch.admin.bit.jme.declaration.JmeDeclarationCreatedEvent;
import ch.admin.bit.jme.test.JmeEnumTestEvent;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@JeapMessageConsumerContracts({
        JmeDeclarationCreatedEvent.TypeRef.class,
        JmeEnumTestEvent.TypeRef.class})
// JmeSimpleTestEvent is consumed from two topics, see SequentialInboxMultiTopicIT
@JeapMessageConsumerContract(value = JmeSimpleTestEvent.TypeRef.class,
        topic = {JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, TestApp.JME_SIMPLE_TEST_EVENT_V2_TOPIC})
public class TestApp {

    public static final String JME_SIMPLE_TEST_EVENT_V2_TOPIC = "jme-simple-test-event-v2";
}
