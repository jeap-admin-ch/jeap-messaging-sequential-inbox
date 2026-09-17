package ch.admin.bit.jeap.messaging.sequentialinbox.inbox;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.Sequence;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class RecordingModeTest {
    @ParameterizedTest
    @CsvSource({
            ",,false", ",2000-01-01T00:00:00,false", ",2099-01-01T00:00:00,true",
            "2099-01-01T00:00:00,,true", "2099-01-01T00:00:00,2000-01-01T00:00:00,true",
            "2000-01-01T00:00:00,2099-01-01T00:00:00,true",
            "2026-01-01T00:00:00,2026-01-01T00:00:00,false"
    })
    void globalOrSequenceTimestamp(String global, String local, boolean expected) {
        SequentialInboxService service = new SequentialInboxService(null, null, null, null, null, null, null);
        service.setSequencingStartTimestamp(global == null ? null : LocalDateTime.parse(global));
        Sequence sequence = new Sequence();
        ReflectionTestUtils.setField(sequence, "sequencingStartTimestamp", local == null ? null : LocalDateTime.parse(local));
        assertThat(service.isRecordingModeEnabled(sequence, LocalDateTime.parse("2026-01-01T00:00:00")))
                .isEqualTo(expected);
    }
}
