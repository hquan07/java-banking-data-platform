package io.github.hquan07.banking.generator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TransactionGeneratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionGeneratorApplication.class, args);
    }
}
