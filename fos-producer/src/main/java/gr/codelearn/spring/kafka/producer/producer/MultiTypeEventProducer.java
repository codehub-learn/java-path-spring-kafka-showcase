package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class MultiTypeEventProducer {

	private final KafkaTemplate<String, Object> producerKafkaTemplate;
	private final TopicsConfig topicsConfig;

	// Sends all three event types from the same template; logs the __TypeId__ header
	// that JacksonJsonSerializer adds so the consumer can reconstruct the correct type
	public void sendOrderLifecycle(OrderPlacedEvent placed, OrderStatusUpdatedEvent statusUpdate,
	                               OrderCancelledEvent cancelled) {
		sendWithTypeHeaderLogging(topicsConfig.placed(), placed.restaurantId(), placed);
		sendWithTypeHeaderLogging(topicsConfig.statusUpdated(), statusUpdate.orderId(), statusUpdate);
		sendWithTypeHeaderLogging(topicsConfig.cancelled(), cancelled.orderId(), cancelled);
	}

	private void sendWithTypeHeaderLogging(String topic, String key, Object event) {
		producerKafkaTemplate.send(topic, key, event)
		                     .whenComplete((result, ex) -> {
								 if (ex != null) {
									 log.error("MultiType send failed type={} key={}: {}",
				                               event.getClass().getSimpleName(), key, ex.getMessage());
								 } else {
									 var meta = result.getRecordMetadata();
									 var typeHeader = result.getProducerRecord().headers().lastHeader("__TypeId__");
									 var typeId = typeHeader != null
				                                  ? new String(typeHeader.value(), StandardCharsets.UTF_8) : "n/a";
									 log.info("MultiType sent type={} key={} topic={} partition={} offset={} " +
				                              "__TypeId__={}",
				                              event.getClass().getSimpleName(), key,
				                              meta.topic(), meta.partition(), meta.offset(), typeId);
								 }
							 });
	}
}
