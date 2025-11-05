package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.MessageHeader;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
interface SpringDataJpaMessageHeaderRepository extends JpaRepository<MessageHeader, Long> {

    @Modifying
    @Query(nativeQuery = true, value = "DELETE FROM message_header WHERE buffered_message_id IN (" +
            "SELECT bm.id FROM buffered_message bm join sequence_instance si on bm.sequence_instance_id = si.id WHERE si.state = 'CLOSED')")
    void deleteForClosedSequences();

    @Query("SELECT mh FROM MessageHeader mh WHERE mh.bufferedMessage.sequencedMessageId = :sequencedMessageId")
    List<MessageHeader> getHeadersForSequencedMessageId(Long sequencedMessageId);


    @Modifying
    @Query(
        nativeQuery = true,
        value = """
            DELETE FROM message_header
            WHERE buffered_message_id IN (
                SELECT bm.id FROM buffered_message bm
                JOIN sequence_instance si ON bm.sequence_instance_id = si.id
                WHERE bm.sequence_instance_id = ?1
                  AND si.state <> 'CLOSED'
            )
            """
    )
    int deleteForNotClosedSequencedId(long sequenceInstanceId);
}
