package ch.admin.bit.jeap.messaging.sequentialinbox.kafka;

import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import jakarta.annotation.PreDestroy;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class DomainEventDeserializer {

    private final Map<String, Deserializer<GenericData.Record>> deserializersByClusterName;

    DomainEventDeserializer(KafkaProperties kafkaProperties, DomainEventDeserializerProvider domainEventDeserializerProvider) {
        this.deserializersByClusterName = kafkaProperties.clusterNames().stream()
                .collect(Collectors.toMap(clusterName -> clusterName,
                        domainEventDeserializerProvider::getGenericRecordDomainEventDeserializer));
    }

    public String toJsonString(String clusterName, String eventTopic, byte[] payload) throws IOException {
        Deserializer<GenericData.Record> deserializer = deserializersByClusterName.get(clusterName);
        GenericData.Record domainEvent = deserializer.deserialize(eventTopic, payload);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(domainEvent.getSchema(), output, true);
        DatumWriter<Object> writer = new GenericDatumWriter<>(domainEvent.getSchema());
        writer.write(domainEvent, encoder);
        encoder.flush();
        output.flush();
        return output.toString(StandardCharsets.UTF_8);
    }

    @PreDestroy
    private void closeKafkaAvroDeserializer() {
        deserializersByClusterName.values().forEach(Deserializer::close);
    }
}
