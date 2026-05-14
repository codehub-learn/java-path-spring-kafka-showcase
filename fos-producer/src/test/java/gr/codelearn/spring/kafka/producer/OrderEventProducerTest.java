package gr.codelearn.spring.kafka.producer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.producer.producer.OrderEventProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "${fos.topics.placed}")
class OrderEventProducerTest {

	@Autowired
	private OrderEventProducer orderEventProducer;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Value("${fos.topics.placed}")
	private String placedTopic;

	private KafkaMessageListenerContainer<String, OrderPlacedEvent> container;
	private BlockingQueue<ConsumerRecord<String, OrderPlacedEvent>> records;

	@BeforeEach
	void setUp() {
		var bootstrapServers = embeddedKafkaBroker.getBrokersAsString();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(bootstrapServers, "test-producer-group");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
		consumerProps.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "gr.codelearn.spring.kafka.domain.*");
		consumerProps.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderPlacedEvent.class.getName());

		var consumerFactory = new DefaultKafkaConsumerFactory<String, OrderPlacedEvent>(
				consumerProps, new StringDeserializer(),
				new JacksonJsonDeserializer<>(OrderPlacedEvent.class));

		container = new KafkaMessageListenerContainer<>(consumerFactory, new ContainerProperties(placedTopic));
		records = new LinkedBlockingQueue<>();
		container.setupMessageListener((MessageListener<String, OrderPlacedEvent>) records::add);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
	}

	@AfterEach
	void tearDown() {
		container.stop();
	}

	@Test
	void send_placedEvent_landsOnCorrectTopicWithRestaurantIdKey() throws InterruptedException {
		var event = new OrderPlacedEvent(
				"ord-test-1", "cust-1", "rest-1",
				List.of("Pizza"), new BigDecimal("9.99"), LocalDateTime.now());

		orderEventProducer.send(event);

		var record = records.poll(10, TimeUnit.SECONDS);
		assertThat(record).isNotNull();
		assertThat(record.topic()).isEqualTo(placedTopic);
		assertThat(record.key()).isEqualTo("rest-1");
		assertThat(record.value().orderId()).isEqualTo("ord-test-1");
		assertThat(record.value().restaurantId()).isEqualTo("rest-1");
	}
}
