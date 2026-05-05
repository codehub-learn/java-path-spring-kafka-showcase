package gr.codelearn.spring.kafka.intro.producer;

import gr.codelearn.spring.kafka.intro.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class HelloMessageProducer {
	private static final Logger log = LogManager.getLogger(HelloMessageProducer.class);

	private final KafkaTemplate<String, String> producerKafkaTemplate;
	private final TopicsConfig topicsConfig;

	public void send(String message) {
		log.info("Sending: {}", message);
		producerKafkaTemplate.send(topicsConfig.hello(), message);
	}
}
