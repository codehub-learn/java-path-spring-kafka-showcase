package gr.codelearn.spring.kafka.generator.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

	private final TopicsConfig topicsConfig;

	@Bean
	public KafkaAdmin.NewTopics generatorTopics() {
		return new KafkaAdmin.NewTopics(
				TopicBuilder.name(topicsConfig.placed())
				            .partitions(12).replicas(3)
				            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build(),
				TopicBuilder.name(topicsConfig.statusUpdated())
				            .partitions(12).replicas(3)
				            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build(),
				TopicBuilder.name(topicsConfig.cancelled())
				            .partitions(3).replicas(3)
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build()
		);
	}
}
