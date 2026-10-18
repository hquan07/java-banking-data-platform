package io.github.hquan07.banking.batch;

import io.minio.MinioClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan
public class BatchProcessorApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(BatchProcessorApplication.class, args);
        int exitCode = SpringApplication.exit(context);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    @Bean
    MinioClient minioClient(BatchProcessorProperties properties) {
        return MinioClient.builder()
                .endpoint(properties.minioEndpoint())
                .credentials(properties.minioAccessKey(), properties.minioSecretKey())
                .build();
    }
}
