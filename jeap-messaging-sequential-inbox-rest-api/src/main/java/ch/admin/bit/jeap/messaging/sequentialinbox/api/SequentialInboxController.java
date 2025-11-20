package ch.admin.bit.jeap.messaging.sequentialinbox.api;

import ch.admin.bit.jeap.messaging.sequentialinbox.api.model.SequenceInstanceDto;
import ch.admin.bit.jeap.messaging.sequentialinbox.api.model.SequencedMessageDto;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstancePendingAction;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessagePendingAction;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
@Slf4j
public class SequentialInboxController {

    private final SequenceInstanceRepository sequenceInstanceRepository;
    private final MessageRepository messageRepository;

    @GetMapping("/sequence/{sequenceName}/{contextId}")
    @Transactional(readOnly = true)
    @PreAuthorize("hasRole('sequentialinbox','view')")
    public ResponseEntity<SequenceInstanceDto> getSequenceDetails(@PathVariable String sequenceName, @PathVariable String contextId) {
        log.info("SequentialInbox: Getting sequence details for sequence '{}' with contextId '{}'", sequenceName, contextId);
        Optional<SequenceInstance> sequenceInstanceOptional = sequenceInstanceRepository.findByNameAndContextId(sequenceName, contextId);
        if (sequenceInstanceOptional.isPresent()) {
            SequenceInstance sequenceInstance = sequenceInstanceOptional.get();
            List<SequencedMessage> messages = messageRepository.findAllBySequenceInstanceId(sequenceInstance.getId());
            log.info("SequentialInbox: Found {} messages for sequence '{}'", messages.size(), sequenceInstance.getId());
            return ResponseEntity.ok(SequenceInstanceDto.fromSequenceInstance(sequenceInstance, messages));
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/sequences/expired")
    @Transactional(readOnly = true)
    @PreAuthorize("hasRole('sequentialinbox','view')")
    public Page<SequenceInstance> getExpiredSequences(@RequestParam(required = false, defaultValue = "0") String pageNumber, @RequestParam(required = false, defaultValue = "10") String pageSize) {
        log.info("SequentialInbox: Getting expired sequences...");
        Page<SequenceInstance> results = sequenceInstanceRepository.findAllExpired(PageRequest.of(Integer.parseInt(pageNumber), Integer.parseInt(pageSize)));
        log.info("SequentialInbox: found {} expired sequences", results.getTotalElements());
        return results;
    }

    @GetMapping("/sequences/expiring")
    @Transactional(readOnly = true)
    @PreAuthorize("hasRole('sequentialinbox','view')")
    public Page<SequenceInstance> getExpiringSequences(@RequestParam(required = false, defaultValue = "0") String pageNumber, @RequestParam(required = false, defaultValue = "10") String pageSize) {
        log.info("SequentialInbox: Getting expiring sequences...");
        Page<SequenceInstance> results = sequenceInstanceRepository.findAllWithRetentionPeriodElapsed75Percent(PageRequest.of(Integer.parseInt(pageNumber), Integer.parseInt(pageSize)));
        log.info("SequentialInbox: found {} expiring sequences", results.getTotalElements());
        return results;
    }

    @PutMapping("/sequence/{sequenceInstanceId}/pending-action")
    @Transactional
    @PreAuthorize("hasRole('sequentialinbox','write')")
    public ResponseEntity<Void> setSequenceInstancePendingAction(@PathVariable long sequenceInstanceId, @RequestParam SequenceInstancePendingAction pendingAction) {
        return sequenceInstanceRepository.findById(sequenceInstanceId)
                .map(sequenceInstance -> {
                    sequenceInstance.setPendingAction(pendingAction);
                    return ResponseEntity.ok().<Void>build();
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/message/{sequencedMessageId}/pending-action")
    @Transactional
    @PreAuthorize("hasRole('sequentialinbox','write')")
    public ResponseEntity<Void> setSequencedMessagePendingAction(@PathVariable long sequencedMessageId, @RequestParam SequencedMessagePendingAction pendingAction) {
        return messageRepository.findById(sequencedMessageId)
                .map(sequencedMessage -> {
                    sequencedMessage.setPendingAction(pendingAction);
                    return ResponseEntity.ok().<Void>build();
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostConstruct
    public void init() {
        log.info("Sequential Inbox REST API Initialized");
    }
}
