package gr.codelearn.spring.kafka.producer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaReplyConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${fos.topics.reply}")
	private String replyTopic;

	private Map<String, Object> replyConsumerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
		props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "gr.codelearn.spring.kafka.domain.*");
		props.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return props;
	}

	@Bean
	public ConsumerFactory<String, Object> replyConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(replyConsumerProps());
	}

	// Used by @KafkaListener in OrderRequestHandler; setReplyTemplate enables @SendTo routing
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> replyKafkaListenerContainerFactory(
			ConsumerFactory<String, Object> replyConsumerFactory,
			KafkaTemplate<String, Object> producerKafkaTemplate) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
		factory.setConsumerFactory(replyConsumerFactory);
		factory.setReplyTemplate(producerKafkaTemplate);
		return factory;
	}

	// Internal container consumed by ReplyingKafkaTemplate to receive replies from the reply topic
	@Bean
	public ConcurrentMessageListenerContainer<String, Object> repliesContainer(
			ConsumerFactory<String, Object> replyConsumerFactory) {
		var containerProps = new ContainerProperties(replyTopic);
		containerProps.setGroupId("fos-producer-reply-group");
		return new ConcurrentMessageListenerContainer<>(replyConsumerFactory, containerProps);
	}

	@Bean
	public ReplyingKafkaTemplate<String, Object, Object> replyingKafkaTemplate(
			ProducerFactory<String, Object> producerFactory,
			ConcurrentMessageListenerContainer<String, Object> repliesContainer) {
		var template = new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);
		template.setDefaultReplyTimeout(Duration.ofSeconds(10));
		return template;
	}
}
