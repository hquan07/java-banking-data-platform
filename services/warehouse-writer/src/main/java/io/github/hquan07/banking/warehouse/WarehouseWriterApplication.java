package io.github.hquan07.banking.warehouse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class WarehouseWriterApplication {

    public static void main(String[] args) {
        SpringApplication.run(WarehouseWriterApplication.class, args);
    }
}
