package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.consumer.service.OrderProcessingService;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderPlacedEventConsumer {

	private final OrderProcessingService orderProcessingService;

	@KafkaListener(
			topics = "${fos.topics.placed}",
			containerFactory = "consumerManualAckKafkaListenerContainerFactory")
	public void receive(ConsumerRecord<String, OrderPlacedEvent> record, Acknowledgment ack) {
		log.debug("Received OrderPlacedEvent  partition={}  offset={}  key={}",
		          record.partition(), record.offset(), record.key());
		orderProcessingService.process(record.value());
		ack.acknowledge();
	}
}
