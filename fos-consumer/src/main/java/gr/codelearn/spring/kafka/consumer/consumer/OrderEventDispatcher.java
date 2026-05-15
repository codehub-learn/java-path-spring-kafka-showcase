package gr.codelearn.spring.kafka.consumer.consumer;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// @KafkaListener at class level + @KafkaHandler per method: Spring dispatches incoming records
// to the method whose parameter type matches the deserialized payload (__TypeId__ header).
// isDefault = true on the catch-all prevents MessageConversionException for unknown types.
@Slf4j
@Component
@KafkaListener(topics = {"${fos.topics.placed}", "${fos.topics.status-updated}"},
               groupId = "fos-consumer-dispatcher",
               containerFactory = "consumerKafkaListenerContainerFactory")
public class OrderEventDispatcher {

	@KafkaHandler
	public void onOrderPlaced(OrderPlacedEvent event) {
		log.info("Dispatcher [placed] orderId={} restaurantId={} items={} total={}",
		         event.orderId(), event.restaurantId(), event.items(), event.totalAmount());
	}

	@KafkaHandler
	public void onOrderStatusUpdated(OrderStatusUpdatedEvent event) {
		log.info("Dispatcher [status] orderId={} {} -> {}",
		         event.orderId(), event.previousStatus(), event.newStatus());
	}

	@KafkaHandler(isDefault = true)
	public void onUnknown(Object event) {
		log.warn("Dispatcher [unknown] unhandled type={} payload={}",
		         event.getClass().getSimpleName(), event);
	}
}
