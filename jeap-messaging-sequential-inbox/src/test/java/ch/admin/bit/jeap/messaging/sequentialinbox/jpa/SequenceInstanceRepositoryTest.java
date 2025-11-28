package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstancePendingAction;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
@ContextConfiguration(classes = SequenceInstanceRepositoryTest.TestConfig.class)
class SequenceInstanceRepositoryTest {

    @Autowired
    private TestEntityManager testEntityManager;

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
        assertThat(deletedCount).isZero();
        // Verify it still exists
        assertThat(jpaSequenceInstanceRepository.findById(closedId)).isPresent();
    }

    @Test
    void deleteNotClosedById_returnsZeroForNonExistentId() {
        // Try to delete a non-existent instance
        int deletedCount = sequenceInstanceRepository.deleteNotClosedById(999999L);

        // Should not delete anything
        assertThat(deletedCount).isZero();
    }

    @Test
    void findAllByPendingActionIsNotNull_returnsInstancesWithPendingAction() {
        SequenceInstance withClose = SequenceInstance.builder()
                .name("pending1")
                .contextId("ctx1")
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(1))
                .build();
        ReflectionTestUtils.setField(withClose, "id", 58485481L);
        ReflectionTestUtils.setField(withClose, "pendingAction", SequenceInstancePendingAction.CLOSE);
        SequenceInstance withConsumeAll = SequenceInstance.builder()
                .name("pending2")
                .contextId("ctx2")
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(1))
                .build();
        ReflectionTestUtils.setField(withConsumeAll, "id", 58485482L);
        ReflectionTestUtils.setField(withConsumeAll, "pendingAction", SequenceInstancePendingAction.CONSUME_ALL);
        SequenceInstance withoutAction = SequenceInstance.builder()
                .name("noPending")
                .contextId("ctx3")
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(1))
                .build();
        ReflectionTestUtils.setField(withoutAction, "id", 58485483L);

        testEntityManager.persist(withClose);
        testEntityManager.persist(withConsumeAll);
        testEntityManager.persist(withoutAction);

        PageRequest pageRequest = PageRequest.of(0, 10);
        var result = sequenceInstanceRepository.findAllByPendingActionIsNotNull(pageRequest);
        assertThat(result)
                .extracting(SequenceInstance::getName)
                .containsExactlyInAnyOrder("pending1", "pending2");
    }

    @Test
    void findAllExpired_returnsOnlyInstancesWithRetainUntilBeforeNow() {
        saveSequenceInstance(17651L, "past1", Duration.ofMinutes(-10));
        saveSequenceInstance(17652L, "past2", Duration.ofMinutes(-10));
        saveSequenceInstance(17653L, "past3", Duration.ofMinutes(-10));
        saveSequenceInstance(17654L, "past4", Duration.ofMinutes(-10));
        saveSequenceInstance(17655L, "future", Duration.ofMinutes(10));

        var pageable = PageRequest.of(0, 10);
        var result = sequenceInstanceRepository.findAllExpired(pageable);

        assertThat(result.getTotalElements()).isEqualTo(4);

        assertThat(result.getContent())
                .extracting(SequenceInstance::getName)
                .doesNotContain("future")
                .contains("past1", "past2", "past3", "past4");
    }

    @Test
    void findByNameAndContextId_returnsCorrectInstance() {
        String name = "testName";
        String contextId = "testContext";
        sequenceInstanceRepository.saveNewInstance(createSequenceInstance(name, contextId));
        sequenceInstanceRepository.saveNewInstance(createSequenceInstance("otherName", "otherContext"));

        Optional<SequenceInstance> result = sequenceInstanceRepository.findByNameAndContextId(name, contextId);

        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo(name);
        assertThat(result.get().getContextId()).isEqualTo(contextId);
    }

    @Test
    void findAllWithRetentionPeriodElapsed75Percent_returnsCorrectInstances() {
        long id = 687770L;
        // Instance with 80% retention elapsed
        saveSequenceInstance(id, "elapsed80", 80, 20);

        // Instance with 50% retention elapsed
        saveSequenceInstance(++id, "elapsed50", 50, 50);

        // Instance with 100% retention elapsed
        saveSequenceInstance(++id, "elapsed100", 100, 0);

        // Instance with 0% retention elapsed
        saveSequenceInstance(++id, "elapsed0", 0, 100);

        var result = sequenceInstanceRepository.findAllWithRetentionPeriodElapsed75Percent(PageRequest.of(0, 1));

        assertThat(result.getTotalElements()).isEqualTo(2);
        assertThat(result.getTotalPages()).isEqualTo(2);

        assertThat(result.getContent())
                .extracting(SequenceInstance::getName)
                .contains("elapsed80")
                .doesNotContain("elapsed50", "elapsed0");

        result = sequenceInstanceRepository.findAllWithRetentionPeriodElapsed75Percent(PageRequest.of(1, 1));

        assertThat(result.getTotalElements()).isEqualTo(2);
        assertThat(result.getTotalPages()).isEqualTo(2);

        assertThat(result.getContent())
                .extracting(SequenceInstance::getName)
                .contains("elapsed100")
                .doesNotContain("elapsed50", "elapsed0");

    }

    @Test
    void getSequenceInstancesExpiringGroupedBySequenceType() {
        long id = 688770L;
        // Instance with 80% retention elapsed
        saveSequenceInstance(id, "elapsed80", 80, 20);

        // Instance with 55% retention elapsed
        saveSequenceInstance(++id, "elapsed55", "ctx1", 55, 45);
        saveSequenceInstance(++id, "elapsed55", "ctx2", 55, 45);

        // Instance with 100% retention elapsed
        saveSequenceInstance(++id, "elapsed100", 100, 0);

        // Instance with 0% retention elapsed
        saveSequenceInstance(++id, "elapsed0", 0, 100);

        Map<String, Long> result = sequenceInstanceRepository.getSequenceInstancesExpiringGroupedBySequenceType(0.5);

        assertThat(result.keySet()).hasSize(2);
        assertThat(result)
                .containsEntry("elapsed80", 1L)
                .containsEntry("elapsed55", 2L);

        result = sequenceInstanceRepository.getSequenceInstancesExpiringGroupedBySequenceType(0.78);

        assertThat(result.keySet()).hasSize(1);
        assertThat(result)
                .containsEntry("elapsed80", 1L);

    }

    @Test
    void getSequenceInstancesWithRetainUntilExpiredGroupedBySequenceType() {
        long id = 698770L;
        // Instance with 80% retention elapsed
        saveSequenceInstance(id, "elapsed80", 80, 20);

        // Instance with 55% retention elapsed
        saveSequenceInstance(++id, "elapsed55", 55, 45);

        // Instance with 100% retention elapsed
        saveSequenceInstance(++id, "elapsed100_1", "ctx1",100, 0);
        saveSequenceInstance(++id, "elapsed100_2", "ctx1",100, 0);
        saveSequenceInstance(++id, "elapsed100_2", "ctx2",100, 0);

        // Instance with 0% retention elapsed
        saveSequenceInstance(++id, "elapsed0", 0, 100);

        Map<String, Long> result = sequenceInstanceRepository.getSequenceInstancesWithRetainUntilExpiredGroupedBySequenceType();

        assertThat(result.keySet()).hasSize(2);
        assertThat(result)
                .containsEntry("elapsed100_1", 1L)
                .containsEntry("elapsed100_2", 2L);

    }

    private void saveSequenceInstance(long id, String name, String contextId, int createdAtDelay, int retainUntil) {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .name(name)
                .contextId(contextId)
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(1))
                .build();
        ReflectionTestUtils.setField(sequenceInstance, "id", id);
        ReflectionTestUtils.setField(sequenceInstance, "createdAt", ZonedDateTime.now().minusMinutes(createdAtDelay));
        ReflectionTestUtils.setField(sequenceInstance, "retainUntil", ZonedDateTime.now().plusMinutes(retainUntil));
        testEntityManager.persist(sequenceInstance);
    }

    private void saveSequenceInstance(long id, String name, int createdAtDelay, int retainUntil) {
        saveSequenceInstance(id, name, "ctx1", createdAtDelay, retainUntil);
    }

    private void saveSequenceInstance(long id, String name, Duration retentionPeriod) {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .name(name)
                .contextId("ctx1")
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(retentionPeriod)
                .build();
        ReflectionTestUtils.setField(sequenceInstance, "id", id);
        testEntityManager.persist(sequenceInstance);
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
    private void createAndSaveSequenceInstanceWithRemovalTime(String name, String contextId, SequenceInstanceState state,
                                                                Duration retentionPeriod, ZonedDateTime removeAfter) {
        long id = createAndSaveSequenceInstance(name, contextId, state, retentionPeriod);
        SequenceInstance instance = jpaSequenceInstanceRepository.findById(id).orElseThrow();
        instance.setRemoveAfter(removeAfter);
        jpaSequenceInstanceRepository.save(instance);
    }

}
