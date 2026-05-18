package gr.codelearn.spring.kafka.generator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fos.generator")
public record GeneratorProperties(
		int ordersPerTick,
		long spawnIntervalMs,
		long advanceIntervalMs,
		int minDwellSeconds,
		int maxDwellSeconds,
		double cancellationProbability,
		double deliveryFailureProbability) {
}
