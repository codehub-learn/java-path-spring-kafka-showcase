package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

	private final KafkaTemplate<String, Object> producerKafkaTemplate;
	private final TopicsConfig topicsConfig;

	public void send(OrderPlacedEvent event) {
		producerKafkaTemplate.send(topicsConfig.placed(), event.restaurantId(), event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("Failed to send OrderPlacedEvent orderId={}: {}", event.orderId(),
				                               ex.getMessage());
								 } else {
									 var meta = result.getRecordMetadata();
									 log.info("OrderPlacedEvent sent  orderId={}  topic={}  partition={}  offset={}",
				                              event.orderId(), meta.topic(), meta.partition(), meta.offset());
								 }
							 });
	}

	public void send(OrderStatusUpdatedEvent event) {
		producerKafkaTemplate.send(topicsConfig.statusUpdated(), event.orderId(), event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("Failed to send OrderStatusUpdatedEvent orderId={}: {}",
				                               event.orderId(),
				                               ex.getMessage());
								 } else {
									 var meta = result.getRecordMetadata();
									 log.info(
											 "OrderStatusUpdatedEvent sent  orderId={}  {} -> {}  topic={}  " +
						                     "partition={}  offset={}",
						                     event.orderId(), event.previousStatus(), event.newStatus(),
						                     meta.topic(), meta.partition(), meta.offset());
								 }
							 });
	}

	public void send(OrderCancelledEvent event) {
		producerKafkaTemplate.send(topicsConfig.cancelled(), event.orderId(), event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("Failed to send OrderCancelledEvent orderId={}: {}", event.orderId(),
				                               ex.getMessage());
								 } else {
									 var meta = result.getRecordMetadata();
									 log.info("OrderCancelledEvent sent  orderId={}  topic={}  partition={}  " +
				                              "offset={}",
				                              event.orderId(), meta.topic(), meta.partition(), meta.offset());
								 }
							 });
	}
}
