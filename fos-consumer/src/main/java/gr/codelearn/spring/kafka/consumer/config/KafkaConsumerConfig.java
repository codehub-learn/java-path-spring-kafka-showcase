package gr.codelearn.spring.kafka.consumer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;

	private Map<String, Object> baseConsumerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
		props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "gr.codelearn.spring.kafka.domain.*");
		props.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, true);
		return props;
	}

	@Bean
	public ConsumerFactory<String, Object> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(baseConsumerProps());
	}

	// Factory 1 — Spring-managed batch ack; filter strips test-keyed records before delivery;
	// replyTemplate enables @SendTo on listener methods that return a value
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> consumerKafkaListenerContainerFactory(
			ConsumerFactory<String, Object> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(3);
		factory.setRecordFilterStrategy(record -> record.key() != null && record.key().startsWith("test-"));
		factory.setReplyTemplate(forwardingKafkaTemplate());
		return factory;
	}

	// Minimal JSON producer used by @SendTo on listeners attached to this factory
	@Bean
	public KafkaTemplate<String, Object> forwardingKafkaTemplate() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
		props.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, true);
		return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
	}

	// Factory 2 — listener calls ack.acknowledge() explicitly after processing each record
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> consumerManualAckKafkaListenerContainerFactory(
			ConsumerFactory<String, Object> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
		factory.setConsumerFactory(consumerFactory);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		factory.getContainerProperties().setPollTimeout(1000L);
		factory.getContainerProperties().setIdleEventInterval(60000L);
		return factory;
	}

	// Factory 3 — listener receives the whole poll batch as a List<ConsumerRecord>
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> consumerBatchKafkaListenerContainerFactory(
			ConsumerFactory<String, Object> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
		factory.setConsumerFactory(consumerFactory);
		factory.setBatchListener(true);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
		factory.setConcurrency(2);
		return factory;
	}

	// Factory 4 — exponential backoff retry, then poison message routed to DLT
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> consumerRetryDltKafkaListenerContainerFactory(
			ConsumerFactory<String, Object> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(retryDltErrorHandler());
		return factory;
	}

	@Bean
	public DefaultErrorHandler retryDltErrorHandler() {
		var backOff = new ExponentialBackOff(1000L, 2.0);
		backOff.setMaxElapsedTime(30000L);
		var recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate());
		return new DefaultErrorHandler(recoverer, backOff);
	}

	// Dedicated byte-passthrough template for DLT — forwards raw bytes from the failed consumer record
	@Bean
	public KafkaTemplate<byte[], byte[]> dltKafkaTemplate() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
	}
}
