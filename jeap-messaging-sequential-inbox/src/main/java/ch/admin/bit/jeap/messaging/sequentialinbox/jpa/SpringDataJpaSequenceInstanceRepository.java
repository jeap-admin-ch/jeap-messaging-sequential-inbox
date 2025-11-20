package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

@Repository
interface SpringDataJpaSequenceInstanceRepository extends JpaRepository<SequenceInstance, Long> {

    @Query(nativeQuery = true, value = "SELECT id FROM sequence_instance WHERE name = ?1 AND context_id = ?2")
    Optional<Long> findIdByNameAndContextId(String name, String contextId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT si FROM SequenceInstance si WHERE si.id = ?1")
    SequenceInstance getByIdAndLockForUpdate(long sequenceInstanceId);

    @Modifying
    @Query(nativeQuery = true, value = "DELETE FROM sequence_instance WHERE state = 'CLOSED'")
    int deleteAllClosed();

    /**
     * Marks all expired sequence instances (retain_until < now()) for delayed removal by setting
     * remove_after = retain_until + delaySeconds
     * @param delaySeconds The delay in seconds to add to retain_until for setting remove_after
     * @return The number of sequence instances marked for delayed removal
     */
    @Modifying
    @Query(nativeQuery = true,
            value = """
                    UPDATE sequence_instance
                    SET remove_after = retain_until + make_interval(secs => :delaySeconds)
                    WHERE remove_after IS NULL AND retain_until < now()
                    """)
    int markExpiredInstancesForDelayedRemoval(@Param("delaySeconds") long delaySeconds);

    /**
     * Fetches a page of sequence instances where remove_after is before the given date ordered by oldest remove_after first.
     *
     * @param date only sequence instances with remove_after before this date are returned
     * @param pageable the Pageable specifying the result limit and paging
     * @return a list of SequenceInstance entities
     */
    List<SequenceInstance> findByRemoveAfterBeforeAndStateNotOrderByRemoveAfterAsc(ZonedDateTime date, SequenceInstanceState state, Pageable pageable);

    @Modifying
    @Query("DELETE FROM SequenceInstance si WHERE si.id = :id AND si.state <> :state")
    int deleteByIdAndStateNot(@Param("id") long sequenceInstanceId, @Param("state") SequenceInstanceState state);

    Slice<SequenceInstance> findAllByPendingActionIsNotNull(Pageable pageable);

    @Query(nativeQuery = true, value = "SELECT * from sequence_instance where created_at + (0.75 * EXTRACT(EPOCH FROM retain_until - created_at)) * INTERVAL '1 second' < now()")
    Page<SequenceInstance> findAllWithRetentionPeriodElapsed75Percent(Pageable pageable);

    Page<SequenceInstance> findAllByRetainUntilBefore(ZonedDateTime retainUntilBefore, Pageable pageable);

    Optional<SequenceInstance> findByNameAndContextId(String name, String contextId);

}
