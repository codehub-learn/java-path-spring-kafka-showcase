package gr.codelearn.spring.kafka.intro.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {
	private final TopicsConfig topicsConfig;

	@Bean
	public NewTopic helloTopic() {
		return TopicBuilder.name(topicsConfig.hello())
		                   .partitions(3)
		                   .replicas(3)
		                   .build();
	}
}
