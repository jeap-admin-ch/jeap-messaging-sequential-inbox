package ch.admin.bit.jeap.messaging.sequentialinbox.configuration.deserializer;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetentionPeriodDeserializerTest {

    @Test
    void deserializeDurationStyle() {
        RetentionPeriodDeserializer deserializer = new RetentionPeriodDeserializer();

        String json = """
                "PT1H30M"
                """;
        Duration duration = deserialize(json, deserializer);

        assertThat(duration)
                .isEqualTo(Duration.ofHours(1).plusMinutes(30));
    }

    @Test
    void deserializeSimpleStyle() {
        RetentionPeriodDeserializer deserializer = new RetentionPeriodDeserializer();

        String json = """
                "1h"
                """;
        Duration duration = deserialize(json, deserializer);

        assertThat(duration)
                .isEqualTo(Duration.ofHours(1));
    }

    @Test
    void deserializeNullValue() {
        RetentionPeriodDeserializer deserializer = new RetentionPeriodDeserializer();

        String json = "null";
        Duration duration = deserialize(json, deserializer);

        assertThat(duration).isNull();
    }

    private static Duration deserialize(String json, RetentionPeriodDeserializer deserializer) {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Duration.class, deserializer);
        ObjectMapper objectMapper = JsonMapper.builder()
                .addModule(module)
                .build();
        return objectMapper.readValue(json, Duration.class);
    }
}
