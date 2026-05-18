package gr.codelearn.spring.kafka.streams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * A generic JSON Serde backed by Spring Kafka's JacksonJsonSerializer / JacksonJsonDeserializer. Used for all domain
 * events and analytics result types in fos-streams topologies.
 */
public class JsonSerde<T> implements Serde<T> {

	private final JacksonJsonSerializer<T> serializer;
	private final JacksonJsonDeserializer<T> deserializer;

	public JsonSerde(Class<T> targetType) {
		this.serializer = new JacksonJsonSerializer<>();
		this.deserializer = new JacksonJsonDeserializer<>(targetType, false);
	}

	@Override
	public Serializer<T> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<T> deserializer() {
		return deserializer;
	}
}
