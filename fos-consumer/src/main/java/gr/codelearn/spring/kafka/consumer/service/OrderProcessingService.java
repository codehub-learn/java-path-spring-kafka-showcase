package gr.codelearn.spring.kafka.consumer.service;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderProcessingService {

	public void process(OrderPlacedEvent event) {
		log.info("Processing order  orderId={}  restaurant={}  customer={}  items={}  total={}",
		         event.orderId(), event.restaurantId(), event.customerId(),
		         event.items(), event.totalAmount());
	}
}
