package gr.codelearn.spring.kafka.streams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fos.topics")
public record TopicsConfig(
		String placed,
		String statusUpdated,
		String cancelled,
		Analytics analytics) {

	public record Analytics(
			String orderEnriched,
			String ordersCompleted,
			String ordersFailed,
			String restaurantOrderCount,
			String restaurantThroughput,
			String restaurantRevenue,
			String courierDeliveries,
			String orderDuration,
			String orderOutcomes,
			String customerSpend) {
	}
}
