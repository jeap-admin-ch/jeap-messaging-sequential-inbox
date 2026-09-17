package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SequentialInboxConfiguration;
import ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping.SequentialInboxHousekeepingService;
import ch.admin.bit.jeap.messaging.sequentialinbox.inbox.ErrorHandlingService;
import ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.IceCreamFlavour;
import ch.admin.bit.jme.test.JmeEnumTestEvent;
import ch.admin.bit.jme.test.JmeSimpleTestEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;

import static ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest.message.TestMessages.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@TestPropertySource(properties = "jeap.messaging.sequential-inbox.config-location=classpath:/messaging/jeap-sequential-inbox-selective-recording.yml")
class SequentialInboxSelectiveRecordingIT extends SequentialInboxITBase {
    @Autowired
    SequentialInboxConfiguration configuration;
    @Autowired
    SequentialInboxHousekeepingService housekeeping;
    @MockitoSpyBean
    ErrorHandlingService errorHandling;

    @Test
    void selectiveRecordingAndExpiry() {
        var recordingContext = randomContextId();
        var activeContext = randomContextId();
        var recordingSequence = configuration.getSequenceByName("RecordingSequence").orElseThrow();
        var timestamp = recordingSequence.getSequencingStartTimestamp();
        try {
            var recorded = createJmeSimpleTestEvent(recordingContext, IceCreamFlavour.VANILLA);
            var buffered = createEnumTestEvent(activeContext);
            sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, recorded);
            sendSync(JmeEnumTestEvent.TypeRef.DEFAULT_TOPIC, buffered);
            assertSequencedMessageProcessedSuccessfully(recorded, "JmeSimpleTestEvent.VANILLA");
            assertMessageConsumedByListener(recorded);
            assertMessageStateWaitingAndBuffered(buffered);
            assertMessageNotConsumedByListener(buffered);
            var recordingInstance = sequenceInstanceRepository.findByNameAndContextId("RecordingSequence", recordingContext.toString()).orElseThrow();
            var activeInstance = sequenceInstanceRepository.findByNameAndContextId("ActiveSequence", activeContext.toString()).orElseThrow();
            assertThat(recordingInstance.isCreatedInRecordingMode()).isTrue();
            assertThat(activeInstance.isCreatedInRecordingMode()).isFalse();
            System.out.println("DEMO 1: RecordingSequence successor PROCESSED; ActiveSequence successor WAITING. Both predecessors missing.");
            demoCheckpoint(1);

            // After activation a different delivery in the same instance must wait for its missing predecessor.
            ReflectionTestUtils.setField(recordingSequence, "sequencingStartTimestamp", LocalDateTime.of(2000, 1, 1, 0, 0));
            var later = createJmeSimpleTestEvent(recordingContext, IceCreamFlavour.VANILLA);
            sendSync(JmeSimpleTestEvent.TypeRef.DEFAULT_TOPIC, later);
            assertMessageStateWaitingAndBuffered(later, "JmeSimpleTestEvent.VANILLA");
            assertThat(sequenceInstanceRepository.findById(recordingInstance.getId()).orElseThrow().isCreatedInRecordingMode()).isTrue();
            // Ensure cleanup also removes payload headers, respecting their foreign key to buffered_message.
            jdbcTemplate.update("""
                    INSERT INTO message_header (id, buffered_message_id, header_name, header_value)
                    SELECT nextval('message_header_sequence'), id, 'demo', decode('01', 'hex')
                    FROM buffered_message WHERE sequence_instance_id IN (?, ?)
                    """, recordingInstance.getId(), activeInstance.getId());
            System.out.println("DEMO 2: Recording ended; a new successor is WAITING, but createdInRecordingMode remains true.");
            demoCheckpoint(2);

            jdbcTemplate.update("UPDATE sequence_instance SET retain_until = NOW() - INTERVAL '1 year' WHERE id IN (?, ?)", recordingInstance.getId(), activeInstance.getId());
            housekeeping.markExpiredSequencesForDelayedRemoval();
            housekeeping.deleteSequencesReadyForRemoval();
            assertThat(sequenceInstanceRepository.findById(recordingInstance.getId())).isEmpty();
            assertThat(sequenceInstanceRepository.findById(activeInstance.getId())).isEmpty();
            for (String table : new String[]{"sequenced_message", "buffered_message", "sequential_inbox_idempotence"}) {
                assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM " + table + " WHERE sequence_instance_id IN (?, ?)", Long.class,
                        recordingInstance.getId(), activeInstance.getId())).isZero();
            }
            verify(errorHandling, never()).sendDeletedSequencedMessageToErrorHandler(argThat(instance -> instance.getId().equals(recordingInstance.getId())), any(), any());
            assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM message_header", Long.class)).isZero();
            verify(errorHandling).sendDeletedSequencedMessageToErrorHandler(argThat(instance -> instance.getId().equals(activeInstance.getId())), any(), any());
            assertMessageSentToErrorHandlingService(buffered);
            System.out.println("DEMO 3: Both expired instances deleted. Only ActiveSequence forwarded to EHS; recording-created instance and its waiting message removed silently.");
            demoCheckpoint(3);
        } finally {
            ReflectionTestUtils.setField(recordingSequence, "sequencingStartTimestamp", timestamp);
        }
    }

    /** Optional local demo: keep the real Kafka/PostgreSQL test alive for inspection between steps. */
    private void demoCheckpoint(int stage) {
        String directory = System.getProperty("jeap6955.demo.directory");
        if (directory == null) {
            return;
        }
        Path state = Path.of(directory);
        try {
            Files.createDirectories(state);
            Files.writeString(state.resolve("status.json"), """
                    {"stage": %d, "container": "%s"}
                    """.formatted(stage, postgres.getContainerId()));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot publish demo checkpoint", e);
        }
        await("Continue demo stage " + stage).atMost(Duration.ofHours(2)).pollInterval(Duration.ofSeconds(1))
                .until(() -> Files.exists(state.resolve("continue-" + stage)));
    }
}
