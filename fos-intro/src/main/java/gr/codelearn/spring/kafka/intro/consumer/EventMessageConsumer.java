package gr.codelearn.spring.kafka.intro.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
public class EventMessageConsumer {
	private static final Logger log = LogManager.getLogger(EventMessageConsumer.class);

	@KafkaListener(
			topics = "${intro.topic.events}",
			groupId = "fos-intro-group",
			containerFactory = "consumerManualAckKafkaListenerContainerFactory",
			id = "event-consumer",
			concurrency = "3"
	)
	public void receive(ConsumerRecord<String, String> record, Acknowledgment ack) {
		LocalDateTime timestamp = LocalDateTime.ofInstant(
				Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
		log.info("Received: listener={}, topic={}, partition={}, offset={}, key={}, value={} at {}",
		         Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(),
		         record.value(), timestamp);
		ack.acknowledge();
	}

	@EventListener
	public void onIdle(ListenerContainerIdleEvent event) {
		log.warn("No messages received for 60 seconds on container: {}", event.getListenerId());
	}
}
