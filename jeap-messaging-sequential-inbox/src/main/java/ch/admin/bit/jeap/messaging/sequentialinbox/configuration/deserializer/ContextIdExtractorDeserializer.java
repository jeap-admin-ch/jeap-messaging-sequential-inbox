package ch.admin.bit.jeap.messaging.sequentialinbox.configuration.deserializer;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.ContextIdExtractor;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

class ContextIdExtractorDeserializer extends StdDeserializer<ContextIdExtractor<?>> {

    public ContextIdExtractorDeserializer() {
        super(ContextIdExtractor.class);
    }

    @Override
    public ContextIdExtractor<?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
        return SequentialInboxConfigurationUtils.newInstance(jsonParser.getValueAsString(), ContextIdExtractor.class);
    }
}
