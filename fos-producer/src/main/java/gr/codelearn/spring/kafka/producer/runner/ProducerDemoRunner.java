package gr.codelearn.spring.kafka.producer.runner;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.producer.MultiTypeEventProducer;
import gr.codelearn.spring.kafka.producer.producer.OrderEventProducer;
import gr.codelearn.spring.kafka.producer.producer.RoutingEventProducer;
import gr.codelearn.spring.kafka.producer.producer.TransactionalOrderEventProducer;
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
	private final TransactionalOrderEventProducer transactionalOrderEventProducer;
	private final MultiTypeEventProducer multiTypeEventProducer;
	private final RoutingEventProducer routingEventProducer;

	@Override
	public void run(String... args) {
		log.info("--- ProducerDemoRunner: starting ---");
		sendDeliveredLifecycle();
		sendCancelledLifecycle();
		sendSyncDemo();
		sendTransactionalDemo();
		sendMultiTypeDemo();
		sendRoutingDemo();
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

	// Demonstrates blocking send: get(5s) returns RecordMetadata directly
	private void sendSyncDemo() {
		log.info("--- [sync send demo] ---");
		orderEventProducer.sendSync(new OrderPlacedEvent(
				"order-demo-sync-1", "cust-003", "rest-003",
				List.of("Souvlaki"), new BigDecimal("7.20"), LocalDateTime.now()));
	}

	// Demonstrates exactly-once semantics: PLACED + ACCEPTED in a single Kafka transaction
	private void sendTransactionalDemo() {
		log.info("--- [transactional send demo] ---");
		var orderId = "order-demo-tx-1";
		var placed = new OrderPlacedEvent(orderId, "cust-004", "rest-004",
		                                  List.of("Caesar Salad"), new BigDecimal("6.50"), LocalDateTime.now());
		var accepted = new OrderStatusUpdatedEvent(orderId, OrderStatus.PLACED, OrderStatus.ACCEPTED,
		                                           LocalDateTime.now());
		transactionalOrderEventProducer.sendAtomically(placed, accepted);
	}

	// Demonstrates multi-type template: same KafkaTemplate sends 3 different POJO types;
	// __TypeId__ header carries the class name so the consumer can deserialise correctly
	private void sendMultiTypeDemo() {
		log.info("--- [multi-type template demo] ---");
		var orderId = "order-demo-multitype-1";
		var placed = new OrderPlacedEvent(orderId, "cust-005", "rest-005",
		                                  List.of("Gyros"), new BigDecimal("5.80"), LocalDateTime.now());
		var statusUpdate = new OrderStatusUpdatedEvent(orderId, OrderStatus.PLACED, OrderStatus.ACCEPTED,
		                                               LocalDateTime.now());
		var cancelled = new OrderCancelledEvent(orderId, "Out of stock", LocalDateTime.now());
		multiTypeEventProducer.sendOrderLifecycle(placed, statusUpdate, cancelled);
	}

	// Demonstrates RoutingKafkaTemplate: topic pattern selects the producer factory at send time
	private void sendRoutingDemo() {
		log.info("--- [routing template demo] ---");
		routingEventProducer.send(new OrderPlacedEvent("order-demo-routing-1", "cust-006", "rest-006",
		                                               List.of("Falafel Wrap"), new BigDecimal("4.90"),
		                                               LocalDateTime.now()));
	}
}
