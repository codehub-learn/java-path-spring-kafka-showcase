package gr.codelearn.spring.kafka.intro.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class HelloMessageConsumer {
	private static final Logger log = LogManager.getLogger(HelloMessageConsumer.class);

	@KafkaListener(
			topics = "${intro.topic.hello}",
			groupId = "fos-intro-group",
			containerFactory = "consumerKafkaListenerContainerFactory"
	)
	public void receive(String message) {
		log.info("Received: {}", message);
	}
}
