package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TransactionalOrderEventProducer {

	private final KafkaTemplate<String, Object> transactionalProducerKafkaTemplate;
	private final TopicsConfig topicsConfig;

	public TransactionalOrderEventProducer(
			@Qualifier("transactionalProducerKafkaTemplate")
			KafkaTemplate<String, Object> transactionalProducerKafkaTemplate,
			TopicsConfig topicsConfig) {
		this.transactionalProducerKafkaTemplate = transactionalProducerKafkaTemplate;
		this.topicsConfig = topicsConfig;
	}

	// Both sends are wrapped in a single Kafka transaction — either both are committed or neither
	public void sendAtomically(OrderPlacedEvent placed, OrderStatusUpdatedEvent accepted) {
		transactionalProducerKafkaTemplate.executeInTransaction(ops -> {
			ops.send(topicsConfig.placed(), placed.restaurantId(), placed);
			ops.send(topicsConfig.statusUpdated(), accepted.orderId(), accepted);
			return null;
		});
		log.info("Transaction committed orderId={} restaurantId={}", placed.orderId(), placed.restaurantId());
	}
}
