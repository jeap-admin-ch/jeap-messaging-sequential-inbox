package ch.admin.bit.jeap.messaging.sequentialinbox.inbox;

import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.kafka.errorhandling.ClusterNameHeaderInterceptor;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.util.Map;
import java.util.Optional;

class FailedConsumerRecord extends ConsumerRecord<Object, Object> {

    private FailedConsumerRecord(SequencedMessage sequencedMessage, Map<String, byte[]> headerMap, Object key, Object value) {
        super(sequencedMessage.getTopic(), 0, 0, 0, TimestampType.NO_TIMESTAMP_TYPE,
                0, 0, key, value, createHeaders(sequencedMessage, headerMap), Optional.empty());
    }

    /**
     * Add the cluster name as a header to the record to make sure the {@link ch.admin.bit.jeap.messaging.kafka.errorhandling.ErrorServiceSender}
     * will produce the {@link ch.admin.bit.jeap.messaging.avro.errorevent.MessageProcessingFailedEvent} to the correct cluster.
     * Also, add any additional headers from the provided map (i.e. signature headers).
     */
    private static Headers createHeaders(SequencedMessage sequencedMessage, Map<String, byte[]> headerMap) {
        RecordHeaders headers = new RecordHeaders();
        if (headerMap != null) {
            headerMap.forEach((key, value) -> headers.add(new RecordHeader(key, value)));
        }
        ClusterNameHeaderInterceptor.addClusterName(headers, sequencedMessage.getClusterName());
        return headers;
    }

    static FailedConsumerRecord of(SequencedMessage sequencedMessage, Map<String, byte[]> headerMap, AvroMessageKey key, AvroMessage value) {
        return new FailedConsumerRecord(sequencedMessage, headerMap, key, value);
    }

    static FailedConsumerRecord of(SequencedMessage sequencedMessage, Map<String, byte[]> headerMap, BufferedMessage bufferedMessage) {
        return new FailedConsumerRecord(sequencedMessage, headerMap, bufferedMessage.getKey(), bufferedMessage.getValue());
    }

    static FailedConsumerRecord of(SequencedMessage sequencedMessage, Map<String, byte[]> headerMap, DeserializedMessage deserializedMessage) {
        return new FailedConsumerRecord(sequencedMessage, headerMap, deserializedMessage.key(), deserializedMessage.message());
    }
}
