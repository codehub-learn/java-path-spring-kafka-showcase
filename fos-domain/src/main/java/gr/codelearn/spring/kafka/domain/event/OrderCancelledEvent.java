package gr.codelearn.spring.kafka.domain.event;

import java.time.LocalDateTime;

public record OrderCancelledEvent(String orderId, String reason, LocalDateTime cancelledAt) {
}
