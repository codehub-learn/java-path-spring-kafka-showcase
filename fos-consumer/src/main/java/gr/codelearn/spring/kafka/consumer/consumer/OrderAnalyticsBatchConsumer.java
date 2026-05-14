package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Component
public class OrderAnalyticsBatchConsumer {

	@KafkaListener(
			topics = "${fos.topics.placed}",
			groupId = "fos-consumer-analytics",
			containerFactory = "consumerBatchKafkaListenerContainerFactory")
	public void receive(List<ConsumerRecord<String, OrderPlacedEvent>> records) {
		var totalRevenue = records.stream()
		                          .map(r -> r.value().totalAmount())
		                          .reduce(BigDecimal.ZERO, BigDecimal::add);
		log.info("Analytics batch  batchSize={}  totalRevenue={}", records.size(), totalRevenue);
	}
}
