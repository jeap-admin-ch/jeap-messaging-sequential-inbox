package ch.admin.bit.jeap.messaging.sequentialinbox.api;

import ch.admin.bit.jeap.messaging.sequentialinbox.api.model.SequenceInstanceDto;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.*;
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
    private final SequenceInstanceDtoFactory sequenceInstanceDtoFactory;

    @GetMapping("/sequences/{sequenceName}/{contextId}")
    @Transactional(readOnly = true)
    @PreAuthorize("hasRole('sequentialinbox','view')")
    public ResponseEntity<SequenceInstanceDto> getSequenceDetails(@PathVariable String sequenceName, @PathVariable String contextId) {
        log.info("SequentialInbox: Getting sequence details for sequence '{}' with contextId '{}'", sequenceName, contextId);
        Optional<SequenceInstance> sequenceInstanceOptional = sequenceInstanceRepository.findByNameAndContextId(sequenceName, contextId);
        if (sequenceInstanceOptional.isPresent()) {
            SequenceInstance sequenceInstance = sequenceInstanceOptional.get();
            List<SequencedMessage> messages = messageRepository.findAllBySequenceInstanceId(sequenceInstance.getId());
            List<BufferedMessage> bufferedMessages = messageRepository.findAllBufferedMessagesBySequenceInstanceId(sequenceInstance.getId());
            log.info("SequentialInbox: found {} messages for sequence '{}'", messages.size(), sequenceInstance.getId());
            return ResponseEntity.ok(sequenceInstanceDtoFactory.fromSequenceInstance(sequenceInstance, messages, bufferedMessages));
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/sequences")
    @Transactional(readOnly = true)
    @PreAuthorize("hasRole('sequentialinbox','view')")
    public Page<SequenceInstance> getExpiredSequences(@RequestParam SequenceInstanceQueryType queryType, @RequestParam(required = false, defaultValue = "0") int pageNumber, @RequestParam(required = false, defaultValue = "10") int pageSize) {
        log.info("SequentialInbox: Getting sequences of type {}...", queryType);
        if (SequenceInstanceQueryType.EXPIRED.equals(queryType)) {
            Page<SequenceInstance> results = sequenceInstanceRepository.findAllExpired(PageRequest.of(pageNumber, pageSize));
            log.info("SequentialInbox: found {} expired sequences", results.getTotalElements());
            return results;
        } else if (SequenceInstanceQueryType.EXPIRING.equals(queryType)) {
            Page<SequenceInstance> results = sequenceInstanceRepository.findAllWithRetentionPeriodElapsed75Percent(PageRequest.of(pageNumber, pageSize));
            log.info("SequentialInbox: found {} expiring sequences", results.getTotalElements());
            return results;
        }
        throw new IllegalStateException("Unsupported query type: " + queryType);
    }

    @PutMapping("/sequences/{sequenceInstanceId}/pending-action")
    @Transactional
    @PreAuthorize("hasRole('sequentialinbox','write')")
    public ResponseEntity<Void> setSequenceInstancePendingAction(@PathVariable long sequenceInstanceId, @RequestParam SequenceInstancePendingAction pendingAction) {
        log.info("SequentialInbox: Set pending action '{}' to sequence instance with id '{}'", pendingAction, sequenceInstanceId);
        return sequenceInstanceRepository.findById(sequenceInstanceId)
                .map(sequenceInstance -> {
                    sequenceInstance.setPendingAction(pendingAction);
                    return ResponseEntity.ok().<Void>build();
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/messages/{sequencedMessageId}/pending-action")
    @Transactional
    @PreAuthorize("hasRole('sequentialinbox','write')")
    public ResponseEntity<Void> setSequencedMessagePendingAction(@PathVariable long sequencedMessageId, @RequestParam SequencedMessagePendingAction pendingAction) {
        log.info("SequentialInbox: Set pending action '{}' to sequenced message with id '{}'", pendingAction, sequencedMessageId);
        return messageRepository.findById(sequencedMessageId)
                .map(sequencedMessage -> {
                    sequencedMessage.setPendingAction(pendingAction);
                    return ResponseEntity.ok().<Void>build();
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostConstruct
    public void init() {
        log.info("SequentialInbox: REST API initialized");
    }
}
