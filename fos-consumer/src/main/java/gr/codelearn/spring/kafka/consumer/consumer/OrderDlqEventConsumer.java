package gr.codelearn.spring.kafka.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class OrderDlqEventConsumer {

	// Reads failed messages forwarded by DeadLetterPublishingRecoverer; extracts DLT headers
	// added by the recoverer so the original context (topic, partition, offset, exception) is visible
	@KafkaListener(topics = "${fos.topics.dlq}",
	               containerFactory = "consumerManualAckKafkaListenerContainerFactory")
	public void receive(ConsumerRecord<String, Object> record, Acknowledgment ack) {
		var h = record.headers();
		log.warn("DLQ message key={} originalTopic={} originalPartition={} originalOffset={} " +
		         "exception={} payload={}",
		         record.key(),
		         headerAsString(h, KafkaHeaders.DLT_ORIGINAL_TOPIC),
		         headerAsInt(h, KafkaHeaders.DLT_ORIGINAL_PARTITION),
		         headerAsLong(h, KafkaHeaders.DLT_ORIGINAL_OFFSET),
		         headerAsString(h, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
		         record.value());
		ack.acknowledge();
	}

	private static String headerAsString(Headers headers, String name) {
		var header = headers.lastHeader(name);
		return header != null ? new String(header.value(), StandardCharsets.UTF_8) : "n/a";
	}

	private static int headerAsInt(Headers headers, String name) {
		var header = headers.lastHeader(name);
		return header != null ? ByteBuffer.wrap(header.value()).getInt() : -1;
	}

	private static long headerAsLong(Headers headers, String name) {
		var header = headers.lastHeader(name);
		return header != null ? ByteBuffer.wrap(header.value()).getLong() : -1L;
	}
}
