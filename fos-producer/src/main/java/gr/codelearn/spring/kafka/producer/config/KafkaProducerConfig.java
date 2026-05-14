package gr.codelearn.spring.kafka.producer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	private Map<String, Object> baseProducerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RestaurantOrderPartitioner.class.getName());
		props.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, true);
		return props;
	}

	@Bean
	@Primary
	public ProducerFactory<String, Object> producerFactory() {
		return new DefaultKafkaProducerFactory<>(baseProducerProps());
	}

	@Bean
	public KafkaTemplate<String, Object> producerKafkaTemplate(
			@Qualifier("producerFactory") ProducerFactory<String, Object> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}

	// Demonstrates exactly-once semantics setup — transactionIdPrefix enables transactions
	@Bean
	public ProducerFactory<String, Object> transactionalProducerFactory() {
		var factory = new DefaultKafkaProducerFactory<String, Object>(baseProducerProps());
		factory.setTransactionIdPrefix("fos-tx-");
		return factory;
	}

	@Bean
	public KafkaTemplate<String, Object> transactionalProducerKafkaTemplate(
			@Qualifier("transactionalProducerFactory") ProducerFactory<String, Object> transactionalProducerFactory) {
		return new KafkaTemplate<>(transactionalProducerFactory);
	}
}
