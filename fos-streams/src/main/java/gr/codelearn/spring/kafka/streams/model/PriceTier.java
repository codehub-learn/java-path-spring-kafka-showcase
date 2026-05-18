package gr.codelearn.spring.kafka.streams.model;

import java.math.BigDecimal;

public enum PriceTier {
	LOW, MEDIUM, HIGH;

	public static PriceTier from(BigDecimal totalAmount) {
		if (totalAmount.compareTo(BigDecimal.TEN) < 0) {
			return LOW;
		}
		if (totalAmount.compareTo(new BigDecimal("25")) < 0) {
			return MEDIUM;
		}
		return HIGH;
	}
}
