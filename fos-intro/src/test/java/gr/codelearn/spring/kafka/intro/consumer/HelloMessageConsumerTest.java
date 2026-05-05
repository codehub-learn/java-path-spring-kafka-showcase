package gr.codelearn.spring.kafka.intro.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 3, topics = "${intro.topic.hello}")
class HelloMessageConsumerTest {
	@Autowired
	private KafkaTemplate<String, String> producerKafkaTemplate;

	@Autowired
	private TestLatch testLatch;

	@Value("${intro.topic.hello}")
	private String helloTopic;

	@Test
	void receive_reactsToIncomingMessage() throws InterruptedException {
		producerKafkaTemplate.send(helloTopic, "test-message");

		boolean received = testLatch.latch().await(10, TimeUnit.SECONDS);
		assertThat(received).isTrue();
	}

	record TestLatch(CountDownLatch latch) {
	}

	@TestConfiguration
	static class Config {
		@Bean
		public TestLatch testLatch() {
			return new TestLatch(new CountDownLatch(1));
		}

		@Bean
		@Primary
		public HelloMessageConsumer helloMessageConsumer(TestLatch testLatch) {
			return new HelloMessageConsumer() {
				@Override
				public void receive(String message) {
					super.receive(message);
					testLatch.latch().countDown();
				}
			};
		}
	}
}
