package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
@ContextConfiguration(classes = SequenceInstanceRepositoryTest.TestConfig.class)
class SequenceInstanceRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @EnableJpaRepositories
    @EntityScan(basePackageClasses = SequenceInstance.class)
    @ComponentScan
    static class TestConfig {
    }

    @Autowired
    private SequenceInstanceRepository sequenceInstanceRepository;
    @Autowired
    private SpringDataJpaSequenceInstanceRepository jpaSequenceInstanceRepository; // For checks

    @Test
    void save_newSequenceInstance() {
        SequenceInstance se = createSequenceInstance("name", "contextId1");
        long persistedSequenceInstanceId = sequenceInstanceRepository.saveNewInstance(se);

        SequenceInstance se2 = createSequenceInstance("name2", "contextId1");
        long persistedSequenceInstance2 = sequenceInstanceRepository.saveNewInstance(se2);
        assertThat(persistedSequenceInstance2)
                .isEqualTo(persistedSequenceInstanceId + 1);
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void save_sameNameContextId_throwsException() {

        sequenceInstanceRepository.saveNewInstance(createSequenceInstance("name", "contextId2"));
        SequenceInstance sequenceInstance = createSequenceInstance("name", "contextId2");

        DataIntegrityViolationException exception = assertThrows(DataIntegrityViolationException.class,
                () -> sequenceInstanceRepository.saveNewInstance(sequenceInstance));

        assertThat(exception.getCause().getMessage())
                .containsIgnoringCase("duplicate key value violates unique constraint \"sequence_instance_name_context_id_uk\"");
    }

    @Test
    void findByTypeAndContextId() {
        String name = UUID.randomUUID().toString();
        String contextId = UUID.randomUUID().toString();
        sequenceInstanceRepository.saveNewInstance(createSequenceInstance(name, contextId));
        sequenceInstanceRepository.saveNewInstance(createSequenceInstance(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        Optional<Long> result = sequenceInstanceRepository.findIdByNameAndContextId(name, contextId);

        assertThat(result).isPresent();
    }

    @Test
    void markExpiredInstancesForDelayedRemoval_marksExpiredInstancesAndReturnsCount() {
        // Create two expired instances and one non-expired instance
        long expired1Id = createAndSaveSequenceInstance("expired1", "ctx1", SequenceInstanceState.OPEN, Duration.ofMinutes(-1));
        long expired2Id = createAndSaveSequenceInstance("expired2", "ctx2", SequenceInstanceState.OPEN, Duration.ofMinutes(-2));
        long notExpiredId = createAndSaveSequenceInstance("notExpired", "ctx3", SequenceInstanceState.OPEN, Duration.ofMinutes(10));
        final long delaySeconds = 600; // 10 minutes

        int markedCount = sequenceInstanceRepository.markExpiredInstancesForDelayedRemoval(delaySeconds);

        // Check that only two instances were marked (the two expired ones)
        assertThat(markedCount).isEqualTo(2);

        // Check remove_after set for expired instances
        SequenceInstance expired1 = jpaSequenceInstanceRepository.findById(expired1Id).orElseThrow();
        assertThat(expired1.getRemoveAfter()).isNotNull();
        assertThat(expired1.getRemoveAfter()).isEqualTo(expired1.getRetainUntil().plusSeconds(delaySeconds));

        SequenceInstance expired2 = jpaSequenceInstanceRepository.findById(expired2Id).orElseThrow();
        assertThat(expired2.getRemoveAfter()).isNotNull();
        assertThat(expired2.getRemoveAfter()).isEqualTo(expired2.getRetainUntil().plusSeconds(delaySeconds));

        // Check remove_after not set for non-expired instance
        SequenceInstance notExpired = jpaSequenceInstanceRepository.findById(notExpiredId).orElseThrow();
        assertThat(notExpired.getRemoveAfter()).isNull();
    }

    @Test
    void findInstancesForRemovalOldestFirst_returnsInstancesOrderedByRemoveAfter() {
        // Create instances with different removeAfter times in the past
        createAndSaveSequenceInstanceWithRemovalTime("middle", "ctx2", SequenceInstanceState.OPEN,
                Duration.ofMinutes(-30), ZonedDateTime.now().minusMinutes(15));
        createAndSaveSequenceInstanceWithRemovalTime("oldest", "ctx1", SequenceInstanceState.OPEN,
                Duration.ofMinutes(-60), ZonedDateTime.now().minusMinutes(30));
        createAndSaveSequenceInstanceWithRemovalTime("newest", "ctx3", SequenceInstanceState.OPEN,
                Duration.ofMinutes(-15), ZonedDateTime.now().minusMinutes(5));

        // Create an instance with removeAfter in the future (should not be returned)
        createAndSaveSequenceInstanceWithRemovalTime("future", "ctx4", SequenceInstanceState.OPEN,
                Duration.ofMinutes(-1), ZonedDateTime.now().plusMinutes(60));

        // Create a CLOSED instance with removeAfter in the past (should not be returned)
        createAndSaveSequenceInstanceWithRemovalTime("closed", "ctx5", SequenceInstanceState.CLOSED,
                Duration.ofMinutes(-120), ZonedDateTime.now().minusMinutes(60));

        // Find instances for removal with a limit of 10
        var result = sequenceInstanceRepository.findInstancesForRemovalOldestFirst(10);

        // Should return 3 instances (oldest, middle, newest) in that order
        assertThat(result).hasSize(3);
        assertThat(result.get(0).getName()).isEqualTo("oldest");
        assertThat(result.get(1).getName()).isEqualTo("middle");
        assertThat(result.get(2).getName()).isEqualTo("newest");
    }

    @Test
    void findInstancesForRemovalOldestFirst_respectsMaxNumInstances() {
        // Create 5 instances all ready for removal
        for (int i = 0; i < 5; i++) {
            createAndSaveSequenceInstanceWithRemovalTime("instance" + i, "ctx" + i, SequenceInstanceState.OPEN,
                    Duration.ofMinutes(-60), ZonedDateTime.now().minusMinutes(30));
        }

        // Request only 3 instances
        var result = sequenceInstanceRepository.findInstancesForRemovalOldestFirst(3);

        // Should return exactly 3 instances
        assertThat(result).hasSize(3);
    }

    @Test
    void deleteNotClosedById_deletesOpenInstance() {
        // Create an OPEN instance
        long openId = createAndSaveSequenceInstance("open", "ctx1", SequenceInstanceState.OPEN, Duration.ofMinutes(1));

        // Delete the open instance
        int deletedCount = sequenceInstanceRepository.deleteNotClosedById(openId);

        // Should delete 1 instance
        assertThat(deletedCount).isEqualTo(1);
        // Verify it's actually deleted
        assertThat(jpaSequenceInstanceRepository.findById(openId)).isEmpty();
    }

    @Test
    void deleteNotClosedById_doesNotDeleteClosedInstance() {
        // Create a CLOSED instance
        long closedId = createAndSaveSequenceInstance("closed", "ctx1", SequenceInstanceState.CLOSED, Duration.ofMinutes(1));

        // Try to delete the closed instance
        int deletedCount = sequenceInstanceRepository.deleteNotClosedById(closedId);

        // Should not delete anything
        assertThat(deletedCount).isEqualTo(0);
        // Verify it still exists
        assertThat(jpaSequenceInstanceRepository.findById(closedId)).isPresent();
    }

    @Test
    void deleteNotClosedById_returnsZeroForNonExistentId() {
        // Try to delete a non-existent instance
        int deletedCount = sequenceInstanceRepository.deleteNotClosedById(999999L);

        // Should not delete anything
        assertThat(deletedCount).isEqualTo(0);
    }

    private SequenceInstance createSequenceInstance(String name, String contextId) {
        return SequenceInstance.builder()
                .name(name)
                .contextId(contextId)
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(1))
                .build();
    }

    /**
     * Creates and saves a sequence instance with the given parameters
     */
    private long createAndSaveSequenceInstance(String name, String contextId, SequenceInstanceState state, Duration retentionPeriod) {
        SequenceInstance instance = SequenceInstance.builder()
                .name(name)
                .contextId(contextId)
                .state(state)
                .retentionPeriod(retentionPeriod)
                .build();
        return sequenceInstanceRepository.saveNewInstance(instance);
    }

    /**
     * Creates and saves a sequence instance, then sets its removeAfter timestamp
     */
    private long createAndSaveSequenceInstanceWithRemovalTime(String name, String contextId, SequenceInstanceState state,
                                                                Duration retentionPeriod, ZonedDateTime removeAfter) {
        long id = createAndSaveSequenceInstance(name, contextId, state, retentionPeriod);
        SequenceInstance instance = jpaSequenceInstanceRepository.findById(id).orElseThrow();
        instance.setRemoveAfter(removeAfter);
        jpaSequenceInstanceRepository.save(instance);
        return id;
    }

}
