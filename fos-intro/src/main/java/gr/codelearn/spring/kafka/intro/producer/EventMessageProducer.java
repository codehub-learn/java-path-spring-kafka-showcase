package gr.codelearn.spring.kafka.intro.producer;

import gr.codelearn.spring.kafka.intro.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventMessageProducer {
	private static final Logger log = LogManager.getLogger(EventMessageProducer.class);

	private final KafkaTemplate<String, String> producerKafkaTemplate;
	private final TopicsConfig topicsConfig;

	public void send(String key, String value) {
		log.info("Sending event: key={}, value={}", key, value);
		producerKafkaTemplate.send(topicsConfig.events(), key, value);
	}
}
