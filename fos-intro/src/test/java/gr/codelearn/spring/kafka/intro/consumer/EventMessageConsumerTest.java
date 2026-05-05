package gr.codelearn.spring.kafka.intro.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 3, topics = {"${intro.topic.hello}", "${intro.topic.events}"})
class EventMessageConsumerTest {
	@Autowired
	private KafkaTemplate<String, String> producerKafkaTemplate;

	@Autowired
	private TestLatch testLatch;

	@Value("${intro.topic.events}")
	private String eventsTopic;

	@Test
	void receive_dumpsKeyAndValue() throws InterruptedException {
		producerKafkaTemplate.send(eventsTopic, "order-1", "placed");

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
		public EventMessageConsumer eventMessageConsumer(TestLatch testLatch) {
			return new EventMessageConsumer() {
				@Override
				public void receive(ConsumerRecord<String, String> record, Acknowledgment ack) {
					super.receive(record, ack);
					testLatch.latch().countDown();
				}
			};
		}
	}
}
