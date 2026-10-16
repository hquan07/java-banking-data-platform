package io.github.hquan07.banking.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@EnableScheduling
@SpringBootApplication
@ConfigurationPropertiesScan
public class OutboxPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutboxPublisherApplication.class, args);
    }

    @Bean
    Clock systemClock() {
        return Clock.systemUTC();
    }
}
