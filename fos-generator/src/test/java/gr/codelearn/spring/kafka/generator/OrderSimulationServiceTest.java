package gr.codelearn.spring.kafka.generator;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.generator.service.OrderSimulationService;
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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "${fos.topics.placed}")
class OrderSimulationServiceTest {

	@Autowired
	OrderSimulationService simulationService;

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Value("${fos.topics.placed}")
	String placedTopic;

	private KafkaMessageListenerContainer<String, OrderPlacedEvent> container;
	private BlockingQueue<ConsumerRecord<String, OrderPlacedEvent>> records;

	@BeforeEach
	void setUp() {
		Map<String, Object> props = KafkaTestUtils.consumerProps(
				embeddedKafkaBroker.getBrokersAsString(), "gen-test-group");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		var factory = new DefaultKafkaConsumerFactory<>(
				props,
				new StringDeserializer(),
				new JacksonJsonDeserializer<>(OrderPlacedEvent.class, false));

		container = new KafkaMessageListenerContainer<>(factory, new ContainerProperties(placedTopic));
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
	void spawnOrders_producesAtLeastOneRecordOnPlacedTopic() throws InterruptedException {
		simulationService.spawnOrders();

		ConsumerRecord<String, OrderPlacedEvent> record = records.poll(10, TimeUnit.SECONDS);
		assertThat(record).isNotNull();
		assertThat(record.topic()).isEqualTo(placedTopic);
		assertThat(record.key()).isNotBlank();
		assertThat(record.value().orderId()).isNotBlank();
		assertThat(record.value().items()).isNotEmpty();
		assertThat(record.value().totalAmount()).isPositive();
	}
}
