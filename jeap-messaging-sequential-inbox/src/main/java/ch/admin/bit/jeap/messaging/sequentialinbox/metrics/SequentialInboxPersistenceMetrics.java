package ch.admin.bit.jeap.messaging.sequentialinbox.metrics;

import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Slf4j
class SequentialInboxPersistenceMetrics {

    private final MessageRepository messageRepository;

    private final SequenceInstanceRepository sequenceInstanceRepository;

    private final double percentile;

    private final Map<String, Long> waitingMessageCountByType = new ConcurrentHashMap<>();

    private final Map<String, Long> expiredSequenceInstancesCountByType = new ConcurrentHashMap<>();

    private final Map<String, Long> expiringSequenceInstancesCountByType = new ConcurrentHashMap<>();

    @Transactional
    public void updateMetrics() {
        log.debug("Updating scheduled inbox metrics");
        updateMetricMap(waitingMessageCountByType, messageRepository.getWaitingMessageCountByType());
        updateMetricMap(expiredSequenceInstancesCountByType, sequenceInstanceRepository.getSequenceInstancesWithRetainUntilExpiredGroupedBySequenceType());
        updateMetricMap(expiringSequenceInstancesCountByType, sequenceInstanceRepository.getSequenceInstancesExpiringGroupedBySequenceType(percentile));
    }

    // Updates the given metric map with new values by clearing the existing entries and putting all new values.
    private void updateMetricMap(Map<String, Long> metricMap, Map<String, Long> newValues) {
        metricMap.clear();
        metricMap.putAll(newValues);
    }

    public long getWaitingMessagesOfType(String type) {
        return waitingMessageCountByType.getOrDefault(type, 0L);
    }

    public long getExpiredSequenceInstancesOfType(String type) {
        return expiredSequenceInstancesCountByType.getOrDefault(type, 0L);
    }

    public long getExpiringSequenceInstancesOfType(String type) {
        return expiringSequenceInstancesCountByType.getOrDefault(type, 0L);
    }
}
