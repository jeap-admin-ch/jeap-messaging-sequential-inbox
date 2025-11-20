package ch.admin.bit.jeap.messaging.sequentialinbox.api;

import ch.admin.bit.jeap.security.test.resource.configuration.ServletJeapAuthorizationConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
class RestApiTestContext extends ServletJeapAuthorizationConfig {

    // You have to provide the system name and the application context to the test support base class.
    RestApiTestContext(ApplicationContext applicationContext) {
        super("jme", applicationContext);
    }


}

