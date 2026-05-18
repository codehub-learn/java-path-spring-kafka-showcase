package gr.codelearn.spring.kafka.streams.config;

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
	public KafkaAdmin.NewTopics analyticsTopics() {
		var analytics = topicsConfig.analytics();
		return new KafkaAdmin.NewTopics(
				TopicBuilder.name(analytics.orderEnriched()).partitions(12).replicas(3).build(),
				TopicBuilder.name(analytics.ordersCompleted()).partitions(6).replicas(3).build(),
				TopicBuilder.name(analytics.ordersFailed()).partitions(3).replicas(3).build(),
				TopicBuilder.name(analytics.restaurantOrderCount())
				            .partitions(6).replicas(3)
				            .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
				            .build(),
				TopicBuilder.name(analytics.restaurantThroughput()).partitions(6).replicas(3).build(),
				TopicBuilder.name(analytics.restaurantRevenue()).partitions(6).replicas(3).build(),
				TopicBuilder.name(analytics.courierDeliveries()).partitions(3).replicas(3).build(),
				TopicBuilder.name(analytics.orderDuration()).partitions(12).replicas(3).build(),
				TopicBuilder.name(analytics.orderOutcomes()).partitions(6).replicas(3).build(),
				TopicBuilder.name(analytics.customerSpend()).partitions(12).replicas(3).build()
		);
	}
}
