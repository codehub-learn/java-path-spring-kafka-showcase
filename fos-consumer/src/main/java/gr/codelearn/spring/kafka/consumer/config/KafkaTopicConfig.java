package gr.codelearn.spring.kafka.consumer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
		configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "fos-consumer-admin");
		return new KafkaAdmin(configs);
	}

	@Bean
	public KafkaAdmin.NewTopics dlqTopic() {
		return new KafkaAdmin.NewTopics(
				TopicBuilder.name(topicsConfig.dlq())
				            .partitions(1)
				            .replicas(3)
				            .build()
		);
	}
}
