package ie.bitstep.mango.swarm.handler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.stereotype.Component;

/**
 * Declares the task type key handled by a {@link TaskHandler} bean and
 * marks the handler as a Spring component.
 */
@Component
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SwarmHandler {
    /**
     * Configured task type key handled by this bean.
     */
    String value();
}
