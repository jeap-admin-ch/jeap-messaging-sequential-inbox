package ch.admin.bit.jeap.messaging.sequentialinbox.configuration.deserializer;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.MessageFilter;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

class MessageFilterDeserializer extends StdDeserializer<MessageFilter<?>> {

    public MessageFilterDeserializer() {
        super(MessageFilter.class);
    }

    @Override
    public MessageFilter<?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
        return SequentialInboxConfigurationUtils.newInstance(jsonParser.getValueAsString(), MessageFilter.class);
    }
}
