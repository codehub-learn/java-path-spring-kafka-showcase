package gr.codelearn.spring.kafka.generator.model;

public enum LifecyclePath {
	/**
	 * PLACED → ACCEPTED → PREPARING → READY → PICKED_UP → DELIVERED
	 */
	HAPPY_PATH,

	/**
	 * PLACED → CANCELLED
	 */
	CANCELLED_EARLY,

	/**
	 * PLACED → ACCEPTED → CANCELLED
	 */
	CANCELLED_LATE,

	/**
	 * PLACED → ACCEPTED → PREPARING → READY → PICKED_UP → DELIVERY_FAILED
	 */
	DELIVERY_FAILED
}
