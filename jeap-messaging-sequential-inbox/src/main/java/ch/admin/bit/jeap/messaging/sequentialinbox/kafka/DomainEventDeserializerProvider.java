package ch.admin.bit.jeap.messaging.sequentialinbox.kafka;

import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import ch.admin.bit.jeap.messaging.kafka.serde.KafkaAvroSerdeProvider;
import ch.admin.bit.jeap.messaging.kafka.serde.confluent.CustomKafkaAvroDeserializer;
import ch.admin.bit.jeap.messaging.kafka.spring.JeapKafkaBeanNames;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.stereotype.Component;

/**
 * Factory creating an Avro deserializer capable of deserializing event payload
 * to {@link org.apache.avro.generic.GenericData.Record}. See {@link CustomKafkaAvroDeserializer} for details.
 * <p>
 * Note: CustomKafkaAvroDeserializer cannot be created as a Spring Bean due to its dependency to kafka server classes
 * which are not on the classpath.
 */
@Component
@RequiredArgsConstructor
public class DomainEventDeserializerProvider implements BeanFactoryAware {

    private final KafkaProperties kafkaProperties;
    @Setter
    private BeanFactory beanFactory;

    @SuppressWarnings("java:S2095")
    // This is a factory method, someone else has to close the Deserializer created here.
    public Deserializer<GenericData.Record> getGenericRecordDomainEventDeserializer(String clusterName) {
        JeapKafkaBeanNames jeapKafkaBeanNames = new JeapKafkaBeanNames(kafkaProperties.getDefaultClusterName());
        KafkaAvroSerdeProvider kafkaAvroSerdeProvider = (KafkaAvroSerdeProvider)
                beanFactory.getBean(jeapKafkaBeanNames.getKafkaAvroSerdeProviderBeanName(clusterName));
        return kafkaAvroSerdeProvider.getGenericDataRecordDeserializerWithoutSignatureCheck();
    }
}
