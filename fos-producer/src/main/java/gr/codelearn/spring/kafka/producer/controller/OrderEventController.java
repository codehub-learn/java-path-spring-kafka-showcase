package gr.codelearn.spring.kafka.producer.controller;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderEventController {

	private final OrderEventProducer orderEventProducer;

	record PlaceOrderRequest(String restaurantId, String customerId, List<String> items, BigDecimal totalAmount) {
	}

	record UpdateOrderStatusRequest(OrderStatus previousStatus, OrderStatus newStatus) {
	}

	record CancelOrderRequest(String reason) {
	}

	@PostMapping
	@ResponseStatus(HttpStatus.ACCEPTED)
	public String placeOrder(@RequestBody PlaceOrderRequest request) {
		var orderId = UUID.randomUUID().toString();
		orderEventProducer.send(new OrderPlacedEvent(
				orderId, request.customerId(), request.restaurantId(),
				request.items(), request.totalAmount(), LocalDateTime.now()));
		return orderId;
	}

	@PutMapping("/{orderId}/status")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void updateStatus(@PathVariable String orderId, @RequestBody UpdateOrderStatusRequest request) {
		orderEventProducer.send(new OrderStatusUpdatedEvent(
				orderId, request.previousStatus(), request.newStatus(), LocalDateTime.now(), null));
	}

	@DeleteMapping("/{orderId}")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void cancel(@PathVariable String orderId, @RequestBody CancelOrderRequest request) {
		orderEventProducer.send(new OrderCancelledEvent(orderId, request.reason(), LocalDateTime.now()));
	}
}
