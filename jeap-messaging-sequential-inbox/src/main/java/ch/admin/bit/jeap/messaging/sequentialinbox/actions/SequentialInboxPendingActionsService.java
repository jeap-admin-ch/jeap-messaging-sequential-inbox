package ch.admin.bit.jeap.messaging.sequentialinbox.actions;

import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.SequentialInboxService;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.BooleanSupplier;

@Slf4j
@Component
public class SequentialInboxPendingActionsService {

    private final MessageRepository messageRepository;
    private final SequenceInstanceRepository sequenceInstanceRepository;
    private final SequentialInboxService sequentialInboxService;
    private final PendingActionsConfigProperties pendingActionsConfigProperties;
    private final Pageable pageable;
    private final TransactionTemplate transactionTemplate;

    public SequentialInboxPendingActionsService(MessageRepository messageRepository, SequenceInstanceRepository sequenceInstanceRepository,
                                                SequentialInboxService sequentialInboxService, PendingActionsConfigProperties pendingActionsConfigProperties,
                                                PlatformTransactionManager transactionManager) {
        this.messageRepository = messageRepository;
        this.sequenceInstanceRepository = sequenceInstanceRepository;
        this.sequentialInboxService = sequentialInboxService;
        this.pendingActionsConfigProperties = pendingActionsConfigProperties;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.pageable = Pageable.ofSize(pendingActionsConfigProperties.getPageSize());
    }

    @Scheduled(cron = "${jeap.messaging.sequential-inbox.pending-actions.messages-cron:0 0/2 * * * *}")
    @SchedulerLock(name = "sequential-inbox-pending-actions-messages", lockAtLeastFor = "#{@pendingActionsConfigProperties.lockAtLeast.toString()}", lockAtMostFor = "#{@pendingActionsConfigProperties.lockAtMost.toString()}")
    public void runPendingActionsOnMessages() {
        log.debug("SequentialInbox: starting running pending actions on messages");
        executeInTransactionPerPage(this::processMessagesWithPendingActions);
        log.debug("SequentialInbox: pending actions completed for messages");
    }

    @Scheduled(cron = "${jeap.messaging.sequential-inbox.pending-actions.sequences-cron:0 1/2 * * * *}")
    @SchedulerLock(name = "sequential-inbox-pending-actions-sequences", lockAtLeastFor = "#{@pendingActionsConfigProperties.lockAtLeast.toString()}", lockAtMostFor = "#{@pendingActionsConfigProperties.lockAtMost.toString()}")
    public void runPendingActionsOnSequences() {
        log.debug("SequentialInbox: starting running pending actions on sequences");
        executeInTransactionPerPage(this::processSequencesWithPendingActions);
        log.debug("SequentialInbox: pending actions completed for sequences");
    }

    private boolean processSequencesWithPendingActions() {
        Slice<SequenceInstance> resultPage = sequenceInstanceRepository.findAllByPendingActionIsNotNull(pageable);
        resultPage.forEach(sequentialInboxService::handleSequenceWithPendingAction);
        return resultPage.hasNext();
    }

    private boolean processMessagesWithPendingActions() {
        Slice<SequencedMessage> resultPage = messageRepository.getMessagesWithPendingAction(pageable);
        resultPage.forEach(sequentialInboxService::handleMessageWithPendingAction);
        return resultPage.hasNext();
    }

    private void executeInTransactionPerPage(BooleanSupplier callback) {
        int pages = 0;
        while (pages < pendingActionsConfigProperties.getMaxPages()) {
            boolean hasMorePages = Boolean.TRUE.equals(transactionTemplate.execute(status -> callback.getAsBoolean()));
            if (!hasMorePages) {
                break;
            }
            pages++;
        }
    }

}
