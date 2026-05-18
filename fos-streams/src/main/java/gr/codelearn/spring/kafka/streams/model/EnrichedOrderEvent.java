package gr.codelearn.spring.kafka.streams.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record EnrichedOrderEvent(
		String orderId,
		String restaurantId,
		String customerId,
		BigDecimal totalAmount,
		PriceTier priceTier,
		LocalDateTime placedAt) {
}
