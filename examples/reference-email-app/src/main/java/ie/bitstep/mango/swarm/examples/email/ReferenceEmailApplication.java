package ie.bitstep.mango.swarm.examples.email;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ReferenceEmailSeedProperties.class)
public class ReferenceEmailApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReferenceEmailApplication.class, args);
    }
}
