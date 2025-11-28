package ch.admin.bit.jeap.messaging.sequentialinbox.metrics;

import ch.admin.bit.jeap.messaging.sequentialinbox.configuration.model.SequentialInboxConfiguration;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration(after = {MetricsAutoConfiguration.class, CompositeMeterRegistryAutoConfiguration.class})
@EnableScheduling
@ConditionalOnBean(MeterRegistry.class)
@Slf4j
class SequentialInboxMetricsAutoConfiguration {

    @Bean
    SequentialInboxMetrics sequentialInboxMetrics(SequentialInboxPersistenceMetrics persistenceMetrics,
                                                  MeterRegistry meterRegistry,
                                                  SequentialInboxConfiguration configuration) {
        return new SequentialInboxMetrics(meterRegistry, configuration, persistenceMetrics);
    }

    @Bean
    SequentialInboxPersistenceMetrics sequentialInboxPersistenceMetrics(MessageRepository messageRepository,
                                                                        SequenceInstanceRepository sequenceInstanceRepository,
                                                                        @Value("${jeap.messaging.sequential-inbox.metrics.expiring-percentile:0.75}") double percentile) {
        log.info("Configuring SequentialInboxPersistenceMetrics with expiring-percentile={}", percentile);
        return new SequentialInboxPersistenceMetrics(messageRepository, sequenceInstanceRepository, percentile);
    }
}
