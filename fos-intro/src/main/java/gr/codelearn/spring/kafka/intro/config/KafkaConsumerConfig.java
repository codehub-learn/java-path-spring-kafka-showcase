package gr.codelearn.spring.kafka.intro.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;
	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;

	private Map<String, Object> consumerProperties() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		// Controls the maximum number of records returned in a single poll() call:
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
		// If the consumer doesn't poll within this interval, the broker considers it dead and triggers a rebalance:
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
		// Broker waits until at least 1KB of data is available
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
		// But won't wait longer than these ms regardless
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return props;
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerProperties());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> consumerKafkaListenerContainerFactory(
			ConsumerFactory<String, String> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(2);

		// Wrap virtual thread executor into Spring's AsyncTaskExecutor
		// ONLY IN CASE VIRTUAL THREADS ARE NOT ENABLED GLOBALLY
		//		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		//		executor.setVirtualThreads(true); // Spring 6.1+ / Boot 3.2+
		//		executor.setThreadNamePrefix("kafka-vt-");
		//		executor.setConcurrencyLimit(3);
		//		factory.getContainerProperties().setListenerTaskExecutor(executor);

		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> consumerManualAckKafkaListenerContainerFactory(
			ConsumerFactory<String, String> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
		factory.setConsumerFactory(consumerFactory);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		factory.getContainerProperties().setPollTimeout(1000L);
		factory.getContainerProperties().setIdleBetweenPolls(250L);
		factory.getContainerProperties().setIdleEventInterval(60000L);
		return factory;
	}
}
