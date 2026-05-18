package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class OrderRequestHandler {

	// Receives the request and returns a reply; @SendTo without a value reads the REPLY_TOPIC
	// header set by ReplyingKafkaTemplate and routes the return value to that topic
	@KafkaListener(topics = "${fos.topics.order-request}",
	               containerFactory = "replyKafkaListenerContainerFactory",
	               groupId = "fos-producer-request-handler")
	@SendTo
	public OrderStatusUpdatedEvent handle(OrderPlacedEvent request) {
		log.info("OrderRequestHandler received orderId={} restaurantId={}", request.orderId(), request.restaurantId());
		return new OrderStatusUpdatedEvent(
				request.orderId(), OrderStatus.PLACED, OrderStatus.ACCEPTED, LocalDateTime.now(), null);
	}
}
