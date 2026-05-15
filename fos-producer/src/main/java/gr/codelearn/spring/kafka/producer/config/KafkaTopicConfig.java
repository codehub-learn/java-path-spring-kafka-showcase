package gr.codelearn.spring.kafka.producer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	private final TopicsConfig topicsConfig;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "fos-producer-admin");
		return new KafkaAdmin(configs);
	}

	@Bean
	public KafkaAdmin.NewTopics fosOrderTopics() {
		return new KafkaAdmin.NewTopics(
				TopicBuilder.name(topicsConfig.placed())
				            .partitions(12)
				            .replicas(3)
				            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 days
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build(),
				TopicBuilder.name(topicsConfig.statusUpdated())
				            .partitions(12)
				            .replicas(3)
				            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build(),
				TopicBuilder.name(topicsConfig.cancelled())
				            .partitions(3)
				            .replicas(3)
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build(),
				TopicBuilder.name(topicsConfig.orderRequest()).partitions(1).replicas(1).build(),
				TopicBuilder.name(topicsConfig.reply()).partitions(1).replicas(1).build()
		);
	}
}
