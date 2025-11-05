package ch.admin.bit.jeap.messaging.sequentialinbox.housekeeping;

import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Data
@Validated
@ConfigurationProperties(prefix = "jeap.messaging.sequential-inbox.housekeeping")
public class HouseKeepingConfigProperties {

    /**
     *  Enable or disable the housekeeping.
     */
    private boolean enabled = true;


    /**
     * The delay period added to the 'retain until' timestamp of a sequence before housekeeping processes it. This buffer
     * allows DevOps engineers time to resolve issues before messages in the sequence are forwarded to the error handling
     * service. Specify the delay as a `Duration` with a maximum precision of seconds.
     */
    @NotNull
    private Duration delay = Duration.ofDays(14);

    /**
     * The maximum duration that a housekeeping process is allowed to run continuously in a single execution.
     * The value cannot exceed 15 minutes.
     */
    @NotNull
    private Duration maxContinuousHouseKeepingDuration = Duration.ofMinutes(15);

    /**
     * The number of sequence instances to process in a single batch during the deletion of expired sequences.
     */
    private int sequenceRemovalBatchSize = 10;

    @PostConstruct
    void checkConfig() {
        if (maxContinuousHouseKeepingDuration.compareTo(Duration.ofMinutes(15)) > 0) {
            throw new IllegalArgumentException("Sequential inbox house keeping configuration error: " +
                    "max-continuous-house-keeping-duration cannot exceed 15 minutes.");
        }
    }

}
