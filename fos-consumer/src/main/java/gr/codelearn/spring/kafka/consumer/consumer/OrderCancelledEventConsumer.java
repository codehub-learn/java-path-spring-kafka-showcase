package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class OrderCancelledEventConsumer {

	private final AtomicInteger receiveCount = new AtomicInteger();
	// Tracks order IDs that triggered a simulated failure; retries for the same ID always re-throw
	// so the error handler exhausts its backoff and hands the record to DeadLetterPublishingRecoverer
	private final Set<String> poisonedOrderIds = ConcurrentHashMap.newKeySet();

	@KafkaListener(
			topics = "${fos.topics.cancelled}",
			containerFactory = "consumerRetryDltKafkaListenerContainerFactory")
	public void receive(ConsumerRecord<String, OrderCancelledEvent> record) {
		var orderId = record.value().orderId();
		if (poisonedOrderIds.contains(orderId)) {
			throw new RuntimeException("Simulated permanent failure orderId=" + orderId);
		}
		if (receiveCount.incrementAndGet() % 3 == 0) {
			poisonedOrderIds.add(orderId);
			throw new RuntimeException("Simulated failure — message will be retried then routed to DLT");
		}
		var event = record.value();
		log.info("Order cancelled orderId={} reason={} cancelledAt={}",
		         event.orderId(), event.reason(), event.cancelledAt());
	}
}
