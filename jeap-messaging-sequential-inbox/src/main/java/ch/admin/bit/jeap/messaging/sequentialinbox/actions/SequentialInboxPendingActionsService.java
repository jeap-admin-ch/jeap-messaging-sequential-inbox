package ch.admin.bit.jeap.messaging.sequentialinbox.actions;

import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.SequentialInboxService;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SequentialInboxPendingActionsService {

    private final MessageRepository messageRepository;
    private final SequenceInstanceRepository sequenceInstanceRepository;
    private final SequentialInboxService sequentialInboxService;

    @Scheduled(cron = "${jeap.messaging.sequential-inbox.pending-actions.messages.cron:0 0/2 * * * *}")
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.REPEATABLE_READ)
    @SchedulerLock(name = "sequential-inbox-pending-actions-messages", lockAtLeastFor = "5s", lockAtMostFor = "1h")
    public void runPendingActionsOnMessages() {
        log.debug("Starting running pending actions on messages");
        List<SequencedMessage> messagesToProcess = messageRepository.getMessagesWithPendingAction();
        messagesToProcess.forEach(sequentialInboxService::handleMessageWithPendingAction);
        if (!messagesToProcess.isEmpty()) {
            log.info("Sequential inbox pending actions completed: messages {} processed", messagesToProcess);
        }
    }

    @Scheduled(cron = "${jeap.messaging.sequential-inbox.pending-actions.sequences.cron:0 1/2 * * * *}")
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.REPEATABLE_READ)
    @SchedulerLock(name = "sequential-inbox-pending-actions-sequences", lockAtLeastFor = "5s", lockAtMostFor = "1h")
    public void runPendingActionsOnSequences() {
        log.debug("Starting running pending actions on sequences");
        List<SequenceInstance> sequencesToProcess = sequenceInstanceRepository.findAllByPendingActionIsNotNull();
        sequencesToProcess.forEach(sequentialInboxService::handleSequenceWithPendingAction);
        if (!sequencesToProcess.isEmpty()) {
            log.info("Sequential inbox pending actions completed: sequences {} processed", sequencesToProcess);
        }
    }

}
