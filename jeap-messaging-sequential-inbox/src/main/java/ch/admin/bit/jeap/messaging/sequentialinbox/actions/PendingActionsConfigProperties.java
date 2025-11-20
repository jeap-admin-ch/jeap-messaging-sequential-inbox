package ch.admin.bit.jeap.messaging.sequentialinbox.actions;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "jeap.messaging.sequential-inbox.pending-actions")
public class PendingActionsConfigProperties {

    /**
     * Minimal time to keep a lock at this job,
     * see {@link net.javacrumbs.shedlock.spring.annotation.SchedulerLock}
     */
    private Duration lockAtLeast = Duration.of(5, ChronoUnit.SECONDS);

    /**
     * Maximal time to keep a lock at this job,
     * see {@link net.javacrumbs.shedlock.spring.annotation.SchedulerLock}
     */
    private Duration lockAtMost = Duration.of(30, ChronoUnit.MINUTES);

    /**
     * Size for the queries [pages]. Default is 50
     */
    private int pageSize = 50;

    /**
     * Max. pages to process in one run. This limits the amount of time one run can max. spend.
     * Default is 10
     */
    private int maxPages = 10;
}

