package gr.codelearn.spring.kafka.producer.runner;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProducerDemoRunner implements CommandLineRunner {

	private final OrderEventProducer orderEventProducer;

	@Override
	public void run(String... args) {
		log.info("--- ProducerDemoRunner: sending sample order lifecycles ---");
		sendDeliveredLifecycle();
		sendCancelledLifecycle();
		log.info("--- ProducerDemoRunner: done ---");
	}

	// order-demo-1: full happy path PLACED → ACCEPTED → PREPARING → READY → PICKED_UP → DELIVERED
	private void sendDeliveredLifecycle() {
		var orderId = "order-demo-1";
		var restaurantId = "rest-001";

		orderEventProducer.send(new OrderPlacedEvent(
				orderId, "cust-001", restaurantId,
				List.of("Margherita Pizza", "Sparkling Water"),
				new BigDecimal("14.50"), LocalDateTime.now()));

		var transitions = List.of(
				new OrderStatus[]{OrderStatus.PLACED, OrderStatus.ACCEPTED},
				new OrderStatus[]{OrderStatus.ACCEPTED, OrderStatus.PREPARING},
				new OrderStatus[]{OrderStatus.PREPARING, OrderStatus.READY},
				new OrderStatus[]{OrderStatus.READY, OrderStatus.PICKED_UP},
				new OrderStatus[]{OrderStatus.PICKED_UP, OrderStatus.DELIVERED}
		                         );
		for (var t : transitions) {
			orderEventProducer.send(new OrderStatusUpdatedEvent(orderId, t[0], t[1], LocalDateTime.now()));
		}
	}

	// order-demo-2: cancelled after acceptance
	private void sendCancelledLifecycle() {
		var orderId = "order-demo-2";
		var restaurantId = "rest-002";

		orderEventProducer.send(new OrderPlacedEvent(
				orderId, "cust-002", restaurantId,
				List.of("BBQ Burger", "Fries"),
				new BigDecimal("11.90"), LocalDateTime.now()));

		orderEventProducer.send(new OrderStatusUpdatedEvent(
				orderId, OrderStatus.PLACED, OrderStatus.ACCEPTED, LocalDateTime.now()));

		orderEventProducer.send(new OrderCancelledEvent(
				orderId, "Customer changed their mind", LocalDateTime.now()));
	}
}
