package gr.codelearn.spring.kafka.domain;

import gr.codelearn.spring.kafka.domain.entity.Order;
import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderDomainTest {

	@Test
	void orderStatusContainsAllExpectedValues() {
		Set<String> names = Arrays.stream(OrderStatus.values())
		                          .map(Enum::name)
		                          .collect(Collectors.toSet());
		assertTrue(names.contains("PLACED"));
		assertTrue(names.contains("ACCEPTED"));
		assertTrue(names.contains("PREPARING"));
		assertTrue(names.contains("READY"));
		assertTrue(names.contains("PICKED_UP"));
		assertTrue(names.contains("DELIVERED"));
		assertTrue(names.contains("CANCELLED"));
		assertTrue(names.contains("DELIVERY_FAILED"));
	}

	@Test
	void orderRecordAccessorsReturnCorrectValues() {
		var order = new Order("ord-1", "rest-1", "cust-1", null, OrderStatus.PLACED,
		                      List.of("Burger", "Fries"), new BigDecimal("12.50"), LocalDateTime.now());
		assertEquals("ord-1", order.orderId());
		assertEquals("rest-1", order.restaurantId());
		assertEquals(OrderStatus.PLACED, order.status());
		assertNull(order.courierId());
	}

	@Test
	void eventRecordEqualityHoldsForIdenticalFields() {
		var now = LocalDateTime.of(2026, 1, 1, 12, 0);

		var placed1 = new OrderPlacedEvent("ord-1", "cust-1", "rest-1", List.of("Pizza"), new BigDecimal("9.99"), now);
		var placed2 = new OrderPlacedEvent("ord-1", "cust-1", "rest-1", List.of("Pizza"), new BigDecimal("9.99"), now);
		assertEquals(placed1, placed2);

		var updated1 = new OrderStatusUpdatedEvent("ord-1", OrderStatus.PLACED, OrderStatus.ACCEPTED, now, null);
		var updated2 = new OrderStatusUpdatedEvent("ord-1", OrderStatus.PLACED, OrderStatus.ACCEPTED, now, null);
		assertEquals(updated1, updated2);

		var cancelled1 = new OrderCancelledEvent("ord-1", "Out of stock", now);
		var cancelled2 = new OrderCancelledEvent("ord-1", "Out of stock", now);
		assertEquals(cancelled1, cancelled2);
	}
}
