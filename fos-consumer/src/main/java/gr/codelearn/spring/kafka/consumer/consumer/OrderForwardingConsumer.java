package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderForwardingConsumer {

	// Consume-transform-forward pattern: the return value is published to the analytics topic
	// by the container's replyTemplate (forwardingKafkaTemplate) — no explicit send() call needed.
	@KafkaListener(topics = "${fos.topics.placed}",
	               groupId = "fos-consumer-forwarder",
	               containerFactory = "consumerKafkaListenerContainerFactory")
	@SendTo("${fos.topics.analytics}")
	public OrderPlacedEvent forward(ConsumerRecord<String, OrderPlacedEvent> record) {
		log.info("Forwarding to analytics orderId={} restaurantId={}",
		         record.value().orderId(), record.value().restaurantId());
		return record.value();
	}
}
