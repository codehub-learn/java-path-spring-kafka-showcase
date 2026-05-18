package gr.codelearn.spring.kafka.generator.service;

import gr.codelearn.spring.kafka.domain.entity.Order;
import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.generator.config.GeneratorProperties;
import gr.codelearn.spring.kafka.generator.model.InFlightOrder;
import gr.codelearn.spring.kafka.generator.model.LifecyclePath;
import gr.codelearn.spring.kafka.generator.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderSimulationService {

	private final ReferenceDataService referenceDataService;
	private final OrderEventProducer orderEventProducer;
	private final GeneratorProperties generatorProperties;

	private final ConcurrentHashMap<String, InFlightOrder> inFlight = new ConcurrentHashMap<>();

	@Scheduled(fixedDelayString = "${fos.generator.spawn-interval-ms}")
	public void spawnOrders() {
		int count = generatorProperties.ordersPerTick();
		log.debug("Spawner tick: creating {} orders (in-flight={})", count, inFlight.size());

		for (int i = 0; i < count; i++) {
			spawnSingleOrder();
		}
	}

	private void spawnSingleOrder() {
		var restaurant = referenceDataService.randomRestaurant();
		var customer = referenceDataService.randomCustomer();
		var items = referenceDataService.randomItems();
		var rng = ThreadLocalRandom.current();

		var totalAmount = items.stream()
		                       .map(item -> BigDecimal.valueOf(3.50 + rng.nextDouble(0, 12.0))
		                                              .setScale(2, RoundingMode.HALF_UP))
		                       .reduce(BigDecimal.ZERO, BigDecimal::add);

		var orderId = UUID.randomUUID().toString();
		var order = new Order(orderId, restaurant.restaurantId(), customer.customerId(),
		                      null, OrderStatus.PLACED, items, totalAmount, LocalDateTime.now());

		var path = choosePath(rng);
		var nextAt = nextTransitionInstant(rng);

		inFlight.put(orderId, new InFlightOrder(order, OrderStatus.PLACED, nextAt, path));

		var event = new OrderPlacedEvent(orderId, customer.customerId(), restaurant.restaurantId(),
		                                 items, totalAmount, LocalDateTime.now());
		orderEventProducer.send(event);
		log.debug("Spawned orderId={} restaurantId={} path={}", orderId, restaurant.restaurantId(), path);
	}

	@Scheduled(fixedDelayString = "${fos.generator.advance-interval-ms}")
	public void advanceOrders() {
		var now = Instant.now();
		inFlight.forEach((orderId, inFlightOrder) -> {
			if (inFlightOrder.nextTransitionAt().isAfter(now)) {
				return;
			}
			advanceSingleOrder(orderId, inFlightOrder);
		});
	}

	private void advanceSingleOrder(String orderId, InFlightOrder inFlightOrder) {
		var current = inFlightOrder.currentStatus();
		var path = inFlightOrder.path();
		var rng = ThreadLocalRandom.current();

		switch (current) {
			case PLACED -> {
				if (path == LifecyclePath.CANCELLED_EARLY) {
					cancel(orderId, inFlightOrder, "Customer cancelled before acceptance");
				} else {
					transition(orderId, inFlightOrder, OrderStatus.ACCEPTED, null, rng);
				}
			}
			case ACCEPTED -> {
				if (path == LifecyclePath.CANCELLED_LATE) {
					cancel(orderId, inFlightOrder, "Customer cancelled after acceptance");
				} else {
					transition(orderId, inFlightOrder, OrderStatus.PREPARING, null, rng);
				}
			}
			case PREPARING -> transition(orderId, inFlightOrder, OrderStatus.READY, null, rng);
			case READY -> {
				var courier = referenceDataService.randomCourier();
				var updated = inFlightOrder.assignCourier(courier.courierId(), nextTransitionInstant(rng));
				inFlight.replace(orderId, updated);
				var event = new OrderStatusUpdatedEvent(orderId, OrderStatus.READY, OrderStatus.PICKED_UP,
				                                        LocalDateTime.now(), courier.courierId());
				orderEventProducer.send(event);
			}
			case PICKED_UP -> {
				var terminal = path == LifecyclePath.DELIVERY_FAILED
				               ? OrderStatus.DELIVERY_FAILED
				               : OrderStatus.DELIVERED;
				transition(orderId, inFlightOrder, terminal, null, rng);
				inFlight.remove(orderId);
			}
			default -> inFlight.remove(orderId);
		}
	}

	private void transition(String orderId, InFlightOrder inFlightOrder,
	                        OrderStatus next, String courierId, ThreadLocalRandom rng) {
		var updated = inFlightOrder.advance(next, nextTransitionInstant(rng));
		inFlight.replace(orderId, updated);
		var event = new OrderStatusUpdatedEvent(orderId, inFlightOrder.currentStatus(), next,
		                                        LocalDateTime.now(), courierId);
		orderEventProducer.send(event);
	}

	private void cancel(String orderId, InFlightOrder inFlightOrder, String reason) {
		inFlight.remove(orderId);
		orderEventProducer.send(new OrderCancelledEvent(orderId, reason, LocalDateTime.now()));
	}

	private LifecyclePath choosePath(ThreadLocalRandom rng) {
		double roll = rng.nextDouble();
		double cancelProb = generatorProperties.cancellationProbability();
		double failProb = generatorProperties.deliveryFailureProbability();

		if (roll < cancelProb / 2.0) {
			return LifecyclePath.CANCELLED_EARLY;
		}
		if (roll < cancelProb) {
			return LifecyclePath.CANCELLED_LATE;
		}
		if (roll < cancelProb + failProb) {
			return LifecyclePath.DELIVERY_FAILED;
		}
		return LifecyclePath.HAPPY_PATH;
	}

	private Instant nextTransitionInstant(ThreadLocalRandom rng) {
		long delay = rng.nextLong(generatorProperties.minDwellSeconds(),
		                          (long) generatorProperties.maxDwellSeconds() + 1);
		return Instant.now().plusSeconds(delay);
	}
}
