package gr.codelearn.spring.kafka.generator.producer;

import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.generator.config.TopicsConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventProducer {

	private final KafkaTemplate<String, Object> producerKafkaTemplate;
	private final TopicsConfig topicsConfig;

	public OrderEventProducer(
			@Qualifier("producerKafkaTemplate") KafkaTemplate<String, Object> producerKafkaTemplate,
			TopicsConfig topicsConfig) {
		this.producerKafkaTemplate = producerKafkaTemplate;
		this.topicsConfig = topicsConfig;
	}

	public void send(OrderPlacedEvent event) {
		producerKafkaTemplate.send(topicsConfig.placed(), event.restaurantId(), event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("Failed to send OrderPlacedEvent orderId={}: {}",
				                               event.orderId(), ex.getMessage());
								 } else {
									 var m = result.getRecordMetadata();
									 log.debug("Sent OrderPlacedEvent orderId={} restaurantId={} part={} offset={}",
				                               event.orderId(), event.restaurantId(), m.partition(), m.offset());
								 }
							 });
	}

	public void send(OrderStatusUpdatedEvent event) {
		producerKafkaTemplate.send(topicsConfig.statusUpdated(), event.orderId(), event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("Failed to send OrderStatusUpdatedEvent orderId={} {} -> {}: {}",
				                               event.orderId(), event.previousStatus(), event.newStatus(),
				                               ex.getMessage());
								 } else {
									 var m = result.getRecordMetadata();
									 log.info("Sent OrderStatusUpdatedEvent orderId={} {} -> {} part={} offset={}",
				                              event.orderId(), event.previousStatus(), event.newStatus(),
				                              m.partition(), m.offset());
								 }
							 });
	}

	public void send(OrderCancelledEvent event) {
		producerKafkaTemplate.send(topicsConfig.cancelled(), event.orderId(), event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("Failed to send OrderCancelledEvent orderId={}: {}",
				                               event.orderId(), ex.getMessage());
								 } else {
									 var m = result.getRecordMetadata();
									 log.info("Sent OrderCancelledEvent orderId={} reason='{}' part={} offset={}",
				                              event.orderId(), event.reason(), m.partition(), m.offset());
								 }
							 });
	}
}
