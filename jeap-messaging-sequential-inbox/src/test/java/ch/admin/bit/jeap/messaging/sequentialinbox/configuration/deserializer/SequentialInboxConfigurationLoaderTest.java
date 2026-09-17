package ch.admin.bit.jeap.messaging.sequentialinbox.configuration.deserializer;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SequentialInboxConfigurationLoaderTest {

    @TempDir
    Path directory;

    @Test
    void loadsSequenceTimestampAndPreservesDurationSyntax() throws Exception {
        Path descriptor = directory.resolve("sequences.yml");
        String source;
        try (var input = getClass().getResourceAsStream("/messaging/jeap-sequential-inbox.yml")) {
            source = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        Files.writeString(descriptor, source.replaceFirst("retentionPeriod:", "sequencingStartTimestamp: '2099-01-01T00:00:00'\n    retentionPeriod:"));
        var config = new SequentialInboxConfigurationLoader(descriptor.toUri().toString()).loadSequenceDeclaration();
        assertThat(config.getSequenceTypes().stream().map(name -> config.getSequenceByName(name).orElseThrow().getSequencingStartTimestamp()))
                .containsExactlyInAnyOrder(null, LocalDateTime.parse("2099-01-01T00:00:00"));
        assertThat(config.getSequenceByQualifiedSequencedMessageTypeName("MyEventType98").getRetentionPeriod()).isEqualTo(Duration.ofHours(6));

        Files.writeString(descriptor, source.replaceFirst("retentionPeriod:", "sequencingStartTimestamp: not-a-timestamp\n    retentionPeriod:"));
        assertThatThrownBy(() -> new SequentialInboxConfigurationLoader(descriptor.toUri().toString()).loadSequenceDeclaration())
                .isInstanceOf(SequentialInboxConfigurationException.class);
    }

    @Test
    void load() {
        SequentialInboxConfigurationLoader loader = new SequentialInboxConfigurationLoader(
                "classpath:/messaging/jeap-sequential-inbox.yml");
        SequentialInboxConfiguration sequentialInboxConfiguration = loader.loadSequenceDeclaration();
        assertThat(sequentialInboxConfiguration).isNotNull();
        assertThat(sequentialInboxConfiguration.getSequenceCount()).isEqualTo(2);

        SequencedMessageType smt = sequentialInboxConfiguration.requireSequencedMessageTypeByQualifiedName("MyEventType98");
        assertThat(smt.getClusterName()).isEqualTo("test-cluster");
        assertThat(smt.getContextIdExtractor()).isInstanceOf(TestContextIdExtractor.class);
        assertThat(smt.getMessageFilter()).isNull();
        Sequence seq = sequentialInboxConfiguration.getSequenceByQualifiedSequencedMessageTypeName("MyEventType98");
        assertThat(seq.getMessages().get(1).getMessageFilter())
                .isInstanceOf(TestMessageFilter.class);
        assertThat(seq.getMessages().get(1).getClusterName())
                .isNull();
        assertThat(seq.getRetentionPeriod())
                .isEqualTo(Duration.ofHours(6));
        assertThat(sequentialInboxConfiguration.getSequenceByName("eventType2AfterEventType1")).isNotNull();
        assertThat(sequentialInboxConfiguration.getSequenceByName("eventType99AfterEventType98")).isNotNull();
        assertThat(sequentialInboxConfiguration.getSequenceByName("fooBar")).isEmpty();
    }
}
