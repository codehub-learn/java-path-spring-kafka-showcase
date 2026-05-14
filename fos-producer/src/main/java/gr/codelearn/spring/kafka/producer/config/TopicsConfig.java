package gr.codelearn.spring.kafka.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fos.topics")
public record TopicsConfig(String placed, String statusUpdated, String cancelled, String dlq) {
}
