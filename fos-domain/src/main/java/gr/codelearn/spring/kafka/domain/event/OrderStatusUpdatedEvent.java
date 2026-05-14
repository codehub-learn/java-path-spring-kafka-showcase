package gr.codelearn.spring.kafka.domain.event;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;

import java.time.LocalDateTime;

public record OrderStatusUpdatedEvent(
		String orderId,
		OrderStatus previousStatus,
		OrderStatus newStatus,
		LocalDateTime updatedAt) {
}
