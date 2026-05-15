package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RoutingEventProducer {

	private final RoutingKafkaTemplate routingKafkaTemplate;
	private final TopicsConfig topicsConfig;

	// RoutingKafkaTemplate selects the ProducerFactory by matching the topic name
	// against the Pattern map in KafkaProducerConfig — JSON factory for business topics,
	// byte-array factory for the DLQ
	public void send(OrderPlacedEvent event) {
		routingKafkaTemplate.send(topicsConfig.placed(), event.restaurantId(), event)
		                    .whenComplete((result, ex) -> {
								if (ex != null) {
									log.error("RoutingTemplate send failed orderId={}: {}",
				                              event.orderId(), ex.getMessage());
								} else {
									var meta = result.getRecordMetadata();
									log.info("RoutingTemplate sent OrderPlacedEvent orderId={} topic={} " +
				                             "partition={} offset={}",
				                             event.orderId(), meta.topic(), meta.partition(), meta.offset());
								}
							});
	}
}
