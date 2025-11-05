package ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping;

import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.*;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Service responsible for performing housekeeping tasks for the sequential inbox.
 * This includes:
 * - Removing expired messages that are past their retention period
 * - Cleaning up closed sequence instances
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SequentialInboxHousekeepingService {

    private final MessageRepository messageRepository;
    private final SequenceInstanceRepository sequenceInstanceRepository;
    private final HouseKeepingConfigProperties houseKeepingConfigProperties;
    private final ErrorHandlingService errorHandlingService;
    private final Transactions transactions;

    /**
     * Scheduled task that runs every 15min (starting at *:00) by default to clean up sequence instances
     * that are in CLOSED state.
     */
    @Scheduled(cron = "${jeap.messaging.sequential-inbox.housekeeping.closed-instances-cron:0 0/15 * * * *}")
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.REPEATABLE_READ)
    @Timed(value = "jeap.messaging.sequential-inbox.housekeeping.closed")
    @SchedulerLock(name = "sequential-inbox-housekeeping-closed", lockAtLeastFor = "5s", lockAtMostFor = "1h")
    public void deleteClosedSequenceInstances() {
        log.debug("Starting housekeeping task: deleting closed sequence instances and related data");

        // Delete in order to respect foreign key constraints
        int messagesDeleted = messageRepository.deleteMessagesForClosedSequences();
        int sequenceInstancesDeleted = sequenceInstanceRepository.deleteAllClosed();

        log.info("Sequential inbox housekeeping completed: deleted {} messages and {} closed sequence instances",
                messagesDeleted, sequenceInstancesDeleted);
    }

    /**
     * Scheduled task that runs every 15min (starting at *:00 + 5min) by default to mark expired sequence instances
     * as for a delayed removal.
     */
    @Scheduled(cron = "${jeap.messaging.sequential-inbox.housekeeping.expiry-cron:0 5/15 * * * *}")
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.REPEATABLE_READ)
    @Timed(value = "jeap.messaging.sequential-inbox.housekeeping.expired")
    @SchedulerLock(name = "sequential-inbox-housekeeping-expired", lockAtLeastFor = "5s", lockAtMostFor = "1h")
    public void markExpiredSequencesForDelayedRemoval() {
        log.debug("Starting housekeeping task: marking expired sequence instances for removal.");

        int instancesMarkedForRemoval = sequenceInstanceRepository.
                markExpiredInstancesForDelayedRemoval(houseKeepingConfigProperties.getDelay().getSeconds());

        log.debug("Sequential inbox housekeeping task completed: marked {} expired sequence instances for removal.",
                instancesMarkedForRemoval);
        if (instancesMarkedForRemoval > 0) {
            log.error("Sequential inbox detected {} sequence instances that have expired.", instancesMarkedForRemoval);
        }
    }

    /**
     * Scheduled task that runs every 15min (starting at *:00 + 10min) by default to remove sequence instances
     * and their messages from the database if they are marked for a delayed removal and the delay period has passed.
     */
    @Scheduled(cron = "${jeap.messaging.sequential-inbox.housekeeping.delete-for-removal-cron:0 10/15 * * * *}")
    @Timed(value = "jeap.messaging.sequential-inbox.housekeeping.delete-for-removal")
    @SchedulerLock(name = "sequential-inbox-housekeeping-delete-for-removal", lockAtLeastFor = "5s", lockAtMostFor = "30m")
    public void deleteSequencesReadyForRemoval() {
        ZonedDateTime startedAt = ZonedDateTime.now();
        ZonedDateTime stopAt = startedAt.plus(houseKeepingConfigProperties.getMaxContinuousHouseKeepingDuration());
        log.debug("Starting housekeeping task: deleting sequence instances ready for removal until {}.", stopAt);
        int deletedSequencesTotal = deleteSequencesReadyForRemovalInBatches(stopAt);
        ZonedDateTime endedAt = ZonedDateTime.now();
        log.debug("Sequential inbox housekeeping task stopped deleting sequence instances ready for removal at {}. " +
                  "Deleted {} sequence instances in {}.", endedAt, deletedSequencesTotal, Duration.between(startedAt, endedAt));
    }

    private int deleteSequencesReadyForRemovalInBatches(ZonedDateTime stopAt) {
        final int batchSize = houseKeepingConfigProperties.getSequenceRemovalBatchSize();
        log.debug("Sequential inbox housekeeping: Starting to delete sequence instances ready for removal in batches of size {}.", batchSize);
        int deletedSequencesTotal = 0;
        int batch = 0;
        do {
            List<SequenceInstance> sequencesReadyToBeRemoved = findInstancesForRemovalBatch(batchSize);
            if (!sequencesReadyToBeRemoved.isEmpty()) {
                batch++;
                log.info("Sequential inbox housekeeping: found {} sequence instances ready for removal in batch {}.",
                        sequencesReadyToBeRemoved.size(), batch);
                int sequencesDeleted = deleteSequencesReadyForRemoval(sequencesReadyToBeRemoved);
                log.info("Sequential inbox housekeeping: deleted {} sequence instances ready for removal in batch {}.",
                        sequencesDeleted, batch);
                deletedSequencesTotal += sequencesDeleted;
            } else {
                log.debug("No more sequence instances ready for removal.");
                break;
            }
        } while (stopAt.isAfter(ZonedDateTime.now()));
        if (deletedSequencesTotal > 0) {
            log.info("Sequential inbox housekeeping: deleted a total of {} sequence instances ready for removal in {} batches.",
                    deletedSequencesTotal, batch);
        }
        return deletedSequencesTotal;
    }

    private List<SequenceInstance> findInstancesForRemovalBatch(int batchSize) {
        return transactions.callInNewTransaction(() ->
            sequenceInstanceRepository.findInstancesForRemovalOldestFirst(batchSize));
    }

    private int deleteSequencesReadyForRemoval(List<SequenceInstance> sequenceInstances) {
        int numSequencesDeleted = 0;
        for (SequenceInstance sequenceInstance : sequenceInstances) {
            boolean deleted = deleteSequenceReadyForRemoval(sequenceInstance);
            if (deleted) {
                numSequencesDeleted++;
            }
        }
        return numSequencesDeleted;
    }

    private boolean deleteSequenceReadyForRemoval(SequenceInstance sequenceInstance) {
        try {
            return transactions.callInNewTransaction(() -> {
                sendSequenceInstanceMessagesToErrorHandlingService(sequenceInstance);
                return deleteSequenceInstance(sequenceInstance);
            });
        } catch (Exception e) {
            log.error("An error occurred trying to delete the sequence instance with id '{}': {}", sequenceInstance.getId(), e.getMessage(), e);
            throw new SequentialInboxHouseKeepingException("An error occurred trying to delete the sequence instance with id '%s'.".
                    formatted(sequenceInstance.getId()), e);
        }
    }

    private void sendSequenceInstanceMessagesToErrorHandlingService(SequenceInstance sequenceInstance) {
        List<SequencedMessage> sequencedMessages = messageRepository.getWaitingMessagesInNewTransaction(sequenceInstance.getId());
        for (SequencedMessage sequencedMessage : sequencedMessages) {
            BufferedMessage message = messageRepository.getBufferedMessageInNewTransaction(sequencedMessage);
            if (message != null) {
                errorHandlingService.sendDeletedSequencedMessageToErrorHandler(sequenceInstance, sequencedMessage, message);
            }
        }
    }

    private boolean deleteSequenceInstance(SequenceInstance sequenceInstance) {
        int messagesDeleted = messageRepository.deleteNotClosedSequenceInstanceMessages(sequenceInstance.getId());
        int instancesDeleted = sequenceInstanceRepository.deleteNotClosedById(sequenceInstance.getId());
        if (instancesDeleted >= 0) {
            log.info("Deleted sequence instance with id '{}' along with {} messages.", sequenceInstance.getId(), messagesDeleted);
        }
        return instancesDeleted != 0;
    }

}
