package ch.admin.bit.jeap.messaging.sequentialinbox.configuration.deserializer;

import org.springframework.boot.convert.DurationStyle;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.time.Duration;

/**
 * Deserializer for {@link Duration} of the sequence instance retention period that supports both ISO format (PT1H30M)
 * and {@link DurationStyle#SIMPLE} (10h).
 */
 class RetentionPeriodDeserializer extends StdDeserializer<Duration> {

     RetentionPeriodDeserializer() {
        super(Duration.class);
    }

    @Override
    public Duration deserialize(JsonParser jp, DeserializationContext ctxt) {
        if (jp.hasToken(JsonToken.VALUE_NULL)) {
            return null;
        }

        String text = jp.getValueAsString();
        DurationStyle durationStyle = DurationStyle.detect(text);
        return durationStyle.parse(text);
    }
}
