package gr.codelearn.spring.kafka.generator.model;

import gr.codelearn.spring.kafka.domain.entity.Order;
import gr.codelearn.spring.kafka.domain.enums.OrderStatus;

import java.time.Instant;

public record InFlightOrder(
		Order order,
		OrderStatus currentStatus,
		Instant nextTransitionAt,
		LifecyclePath path) {

	public InFlightOrder advance(OrderStatus newStatus, Instant nextAt) {
		var updated = new Order(order.orderId(), order.restaurantId(), order.customerId(),
		                        order.courierId(), newStatus, order.items(),
		                        order.totalAmount(), order.createdAt());
		return new InFlightOrder(updated, newStatus, nextAt, path);
	}

	public InFlightOrder assignCourier(String courierId, Instant nextAt) {
		var updated = new Order(order.orderId(), order.restaurantId(), order.customerId(),
		                        courierId, OrderStatus.PICKED_UP, order.items(),
		                        order.totalAmount(), order.createdAt());
		return new InFlightOrder(updated, OrderStatus.PICKED_UP, nextAt, path);
	}
}
