package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class OrderCancelledEventConsumer {

	// every 3rd message throws to demonstrate retry + DLT routing
	private final AtomicInteger receiveCount = new AtomicInteger();

	@KafkaListener(
			topics = "${fos.topics.cancelled}",
			containerFactory = "consumerRetryDltKafkaListenerContainerFactory")
	public void receive(ConsumerRecord<String, OrderCancelledEvent> record) {
		if (receiveCount.incrementAndGet() % 3 == 0) {
			throw new RuntimeException("Simulated failure — message will be retried then routed to DLT");
		}
		var event = record.value();
		log.info("Order cancelled  orderId={}  reason={}  cancelledAt={}",
		         event.orderId(), event.reason(), event.cancelledAt());
	}
}
