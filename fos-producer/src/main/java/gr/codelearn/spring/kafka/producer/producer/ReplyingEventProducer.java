package gr.codelearn.spring.kafka.producer.producer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReplyingEventProducer {

	private final ReplyingKafkaTemplate<String, Object, Object> replyingKafkaTemplate;
	private final TopicsConfig topicsConfig;

	// Sends a request to the order-request topic and awaits a reply on the reply topic.
	// ReplyingKafkaTemplate stamps REPLY_TOPIC and CORRELATION_ID onto the ProducerRecord
	// before the async send — both are readable on the record immediately after sendAndReceive().
	public void sendAndReceive(OrderPlacedEvent event) {
		var record = new ProducerRecord<String, Object>(topicsConfig.orderRequest(), event.restaurantId(), event);
		var future = replyingKafkaTemplate.sendAndReceive(record);

		log.info("ReplyingTemplate request sent orderId={} {}={} {}={}",
		         event.orderId(),
		         KafkaHeaders.REPLY_TOPIC,
		         headerAsString(record.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)),
		         KafkaHeaders.CORRELATION_ID,
		         headerAsHex(record.headers().lastHeader(KafkaHeaders.CORRELATION_ID)));

		future.whenComplete((reply, ex) -> {
			if (ex != null) {
				log.error("ReplyingTemplate failed orderId={}: {}", event.orderId(), ex.getMessage());
			} else {
				log.info("ReplyingTemplate reply received orderId={} reply={}", event.orderId(), reply.value());
			}
		});
	}

	private static String headerAsString(Header header) {
		return header != null ? new String(header.value(), StandardCharsets.UTF_8) : "n/a";
	}

	private static String headerAsHex(Header header) {
		return header != null ? HexFormat.of().formatHex(header.value()) : "n/a";
	}
}
