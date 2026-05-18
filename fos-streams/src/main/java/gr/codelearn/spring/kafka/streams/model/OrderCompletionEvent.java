package gr.codelearn.spring.kafka.streams.model;

public record OrderCompletionEvent(
		String orderId,
		String restaurantId,
		String customerId,
		String outcome,
		long durationSeconds) {
}
