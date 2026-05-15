package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderStatusEventConsumer {

	@KafkaListener(
			topics = "${fos.topics.status-updated}",
			containerFactory = "consumerKafkaListenerContainerFactory",
			autoStartup = "${fos.consumers.status-consumer.auto-start:false}")
	public void receive(ConsumerRecord<String, OrderStatusUpdatedEvent> record) {
		var event = record.value();
		log.info("Order status updated orderId={} {} -> {}",
		         event.orderId(), event.previousStatus(), event.newStatus());
	}
}
