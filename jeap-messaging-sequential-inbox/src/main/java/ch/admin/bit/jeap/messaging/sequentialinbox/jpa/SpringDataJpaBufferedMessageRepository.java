package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.BufferedMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
interface SpringDataJpaBufferedMessageRepository extends JpaRepository<BufferedMessage, Long> {

    @Modifying
    @Query(nativeQuery = true, value = "DELETE FROM buffered_message WHERE sequence_instance_id IN (SELECT id FROM sequence_instance WHERE state = 'CLOSED')")
    void deleteForClosedSequences();

    @Query("from BufferedMessage bm left join fetch bm.headers where bm.sequencedMessageId = :sequencedMessageId")
    BufferedMessage getBySequencedMessageId(long sequencedMessageId);

    @Modifying
    @Query(
            nativeQuery = true,
            value = """
                DELETE FROM buffered_message
                WHERE sequence_instance_id = ?1
                  AND EXISTS (
                      SELECT 1 FROM sequence_instance WHERE id = ?1 AND state <> 'CLOSED'
                )
                """
    )
    int deleteForNotClosedSequencedId(long sequenceInstanceId);

}
