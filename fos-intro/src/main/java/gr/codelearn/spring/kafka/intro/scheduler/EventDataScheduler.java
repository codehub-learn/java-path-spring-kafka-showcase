package gr.codelearn.spring.kafka.intro.scheduler;

import gr.codelearn.spring.kafka.intro.producer.EventMessageProducer;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
public class EventDataScheduler {
	private static final Logger log = LogManager.getLogger(EventDataScheduler.class);
	private static final List<String> EVENT_TYPES =
			List.of("PLACED", "ACCEPTED", "PREPARING", "READY", "PICKED_UP", "DELIVERED");

	private final EventMessageProducer eventMessageProducer;
	private final AtomicLong counter = new AtomicLong();

	@Value("${intro.scheduler.batch-size:1}")
	private int batchSize;

	@Scheduled(fixedRateString = "${intro.scheduler.event-rate-ms:2000}")
	public void generate() {
		for (int i = 0; i < batchSize; i++) {
			long id = counter.incrementAndGet();
			String key = "order-" + id;
			String value = EVENT_TYPES.get((int) (id % EVENT_TYPES.size()));
			log.debug("Scheduling event: key={}, value={}", key, value);
			eventMessageProducer.send(key, value);
		}
	}
}
