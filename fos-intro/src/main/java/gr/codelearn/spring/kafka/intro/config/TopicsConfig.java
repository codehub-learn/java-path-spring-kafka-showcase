package gr.codelearn.spring.kafka.intro.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "intro.topic")
public record TopicsConfig(String hello, String events, String dlq) {
}
