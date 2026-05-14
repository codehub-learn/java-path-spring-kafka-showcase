package gr.codelearn.spring.kafka.domain.entity;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record Order(
		String orderId,
		String restaurantId,
		String customerId,
		String courierId,
		OrderStatus status,
		List<String> items,
		BigDecimal totalAmount,
		LocalDateTime createdAt) {
}
