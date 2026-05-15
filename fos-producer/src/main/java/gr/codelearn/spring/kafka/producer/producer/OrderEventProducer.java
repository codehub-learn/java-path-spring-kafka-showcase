package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

	// Blocking variant — blocks until the broker acknowledges or the 5 s timeout expires
	public void sendSync(OrderPlacedEvent event) {
		try {
			var result = producerKafkaTemplate
					.send(topicsConfig.placed(), event.restaurantId(), event)
					.get(5, TimeUnit.SECONDS);
			var meta = result.getRecordMetadata();
			log.info("OrderPlacedEvent [sync]  orderId={}  topic={}  partition={}  offset={}",
			         event.orderId(), meta.topic(), meta.partition(), meta.offset());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.error("Interrupted waiting for send result orderId={}", event.orderId());
		} catch (ExecutionException | TimeoutException e) {
			log.error("Send failed [sync] orderId={}: {}", event.orderId(), e.getMessage());
		}
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
											 "OrderStatusUpdatedEvent sent orderId={} {} -> {} topic={} " +
						                     "partition={} offset={}",
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
									 log.info("OrderCancelledEvent sent orderId={} topic={} partition={}  " +
				                              "offset={}",
				                              event.orderId(), meta.topic(), meta.partition(), meta.offset());
								 }
							 });
	}
}
