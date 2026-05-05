package gr.codelearn.spring.kafka.intro.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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

	// Explicit KafkaAdmin — Spring Boot auto-configures one, but showing manual creation
	// is useful when custom AdminClientConfig properties are needed (e.g. auth, timeouts)
	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		return new KafkaAdmin(configs);
	}

	// Approach 1: single @Bean method returning NewTopic
	@Bean
	public NewTopic helloTopic() {
		return TopicBuilder.name(topicsConfig.hello())
		                   .partitions(3)
		                   .replicas(3)
		                   .build();
	}

	// Approach 2: single @Bean method returning KafkaAdmin.NewTopics — groups multiple topics together
	@Bean
	public KafkaAdmin.NewTopics introTopics() {
		return new KafkaAdmin.NewTopics(
				TopicBuilder.name(topicsConfig.events())
				            .partitions(3)
				            .replicas(3)
				            .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
				            .config(TopicConfig.RETENTION_BYTES_CONFIG, "256000")
				            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
				            .build(),
				TopicBuilder.name(topicsConfig.dlq())
				            .partitions(1)
				            .replicas(3)
				            .build()
		);
	}
}
