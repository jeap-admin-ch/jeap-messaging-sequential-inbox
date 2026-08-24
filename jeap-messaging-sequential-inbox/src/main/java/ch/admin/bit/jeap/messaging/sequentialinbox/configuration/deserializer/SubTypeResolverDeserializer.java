package ch.admin.bit.jeap.messaging.sequentialinbox.configuration.deserializer;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SubTypeResolver;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

class SubTypeResolverDeserializer extends StdDeserializer<SubTypeResolver<?, ?>> {

    public SubTypeResolverDeserializer() {
        super(SubTypeResolver.class);
    }

    @Override
    public SubTypeResolver<?, ?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
        return SequentialInboxConfigurationUtils.newInstance(jsonParser.getValueAsString(), SubTypeResolver.class);
    }
}
