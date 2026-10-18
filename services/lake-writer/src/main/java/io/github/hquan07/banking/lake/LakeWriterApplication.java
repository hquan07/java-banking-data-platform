package io.github.hquan07.banking.lake;

import io.minio.MinioClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan
public class LakeWriterApplication {

    public static void main(String[] args) {
        SpringApplication.run(LakeWriterApplication.class, args);
    }

    @Bean
    MinioClient minioClient(LakeWriterProperties properties) {
        return MinioClient.builder()
                .endpoint(properties.endpoint())
                .credentials(properties.accessKey(), properties.secretKey())
                .build();
    }
}
