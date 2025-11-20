package ch.admin.bit.jeap.messaging.sequentialinbox.api.config;

import ch.admin.bit.jeap.messaging.sequentialinbox.api.SequentialInboxController;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan(basePackageClasses =  SequentialInboxController.class)
class SequentialInboxRestApiAutoConfiguration {

}
