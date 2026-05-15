package gr.codelearn.spring.kafka.consumer;

import gr.codelearn.spring.kafka.consumer.service.OrderProcessingService;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "fos.consumers.placed-consumer.auto-start=true")
@DirtiesContext
@EmbeddedKafka(partitions = 3, topics = "${fos.topics.placed}")
class OrderPlacedEventConsumerTest {

	@Autowired
	private KafkaTemplate<String, Object> testProducerKafkaTemplate;

	@Autowired
	private TestLatch testLatch;

	@Value("${fos.topics.placed}")
	private String placedTopic;

	@Test
	void receive_processesIncomingOrderPlacedEvent() throws InterruptedException {
		var event = new OrderPlacedEvent(
				"ord-consumer-test-1", "cust-1", "rest-1",
				List.of("Spaghetti"), new BigDecimal("8.50"), LocalDateTime.now());

		testProducerKafkaTemplate.send(placedTopic, event.restaurantId(), event);

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
		public OrderProcessingService orderProcessingService(TestLatch testLatch) {
			return new OrderProcessingService() {
				@Override
				public void process(OrderPlacedEvent event) {
					super.process(event);
					testLatch.latch().countDown();
				}
			};
		}

		@Bean
		public KafkaTemplate<String, Object> testProducerKafkaTemplate(
				@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
			Map<String, Object> props = new HashMap<>();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
			props.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, true);
			return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
		}
	}
}
