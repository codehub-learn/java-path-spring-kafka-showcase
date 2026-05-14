package gr.codelearn.spring.kafka.domain.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record OrderPlacedEvent(
		String orderId,
		String customerId,
		String restaurantId,
		List<String> items,
		BigDecimal totalAmount,
		LocalDateTime placedAt) {
}
