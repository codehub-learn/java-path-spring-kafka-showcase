package gr.codelearn.spring.kafka.intro.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "${intro.topic.hello}")
class HelloMessageProducerTest {
	@Autowired
	private HelloMessageProducer helloMessageProducer;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Value("${intro.topic.hello}")
	private String helloTopic;

	private KafkaMessageListenerContainer<String, String> container;
	private BlockingQueue<ConsumerRecord<String, String>> records;

	@BeforeEach
	void setUp() {
		var bootstrapServers = embeddedKafkaBroker.getBrokersAsString();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(bootstrapServers, "test-producer-group");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
		container = new KafkaMessageListenerContainer<>(consumerFactory, new ContainerProperties(helloTopic));
		records = new LinkedBlockingQueue<>();
		container.setupMessageListener((MessageListener<String, String>) records::add);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
	}

	@AfterEach
	void tearDown() {
		container.stop();
	}

	@Test
	void send_deliversMessageToTopic() {
		helloMessageProducer.send("ping");

		await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
			ConsumerRecord<String, String> record = records.poll(100, TimeUnit.MILLISECONDS);
			assertThat(record).isNotNull();
			assertThat(record.value()).isEqualTo("ping");
		});
	}
}
