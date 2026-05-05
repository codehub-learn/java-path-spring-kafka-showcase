package gr.codelearn.spring.kafka.intro.runner;

import gr.codelearn.spring.kafka.intro.producer.HelloMessageProducer;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class IntroRunner implements CommandLineRunner {
	private static final Logger log = LogManager.getLogger(IntroRunner.class);

	private final HelloMessageProducer helloMessageProducer;

	@Override
	public void run(String... args) {
		log.info("Sending demo messages...");
		helloMessageProducer.send("Hello, Kafka!");
		helloMessageProducer.send("Spring Kafka basics are running.");
	}
}
