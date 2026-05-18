package gr.codelearn.spring.kafka.streams.model;

public record OutcomeEvent(
		String orderId,
		String outcome) {
}
