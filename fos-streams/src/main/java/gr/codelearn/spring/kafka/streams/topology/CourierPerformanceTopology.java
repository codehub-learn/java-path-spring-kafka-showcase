package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/*
 * T6 — Level 3 (Windowed): demonstrates selectKey() to rekey a stream before grouping,
 * and SlidingWindows (event-time based, overlapping) vs TumblingWindows (non-overlapping).
 *
 * For each PICKED_UP event, counts how many pickups the same courier performed
 * within the preceding 10-minute sliding window. Each incoming event creates a
 * window from (event.time - 10 min) to event.time — the window slides with each event.
 *
 * selectKey() changes the record key from orderId → courierId, which triggers an
 * implicit repartition so records for the same courier land on the same partition.
 * Only PICKED_UP events carry a non-null courierId, so we filter first.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class CourierPerformanceTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KTable<Windowed<String>, Long> courierPerformanceTable(StreamsBuilder streamsBuilder) {
		KTable<Windowed<String>, Long> performanceTable = streamsBuilder
				.stream(topicsConfig.statusUpdated(),
				        Consumed.with(Serdes.String(), new JsonSerde<>(OrderStatusUpdatedEvent.class)))
				.filter((key, event) -> event.newStatus() == OrderStatus.PICKED_UP
				                        && event.courierId() != null)
				// Rekeying triggers an implicit repartition topic (courierId is not the original key)
				.selectKey((key, event) -> event.courierId())
				.groupByKey()
				.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)))
				.count(Materialized
							   .<String, Long, WindowStore<Bytes, byte[]>>as("courier-performance-store")
						       .withKeySerde(Serdes.String())
						       .withValueSerde(Serdes.Long()));

		performanceTable
				.toStream()
				.peek((windowedKey, count) ->
							  log.info("Courier {} pickups in 10-min window ending {}: {}",
						               windowedKey.key(), windowedKey.window().endTime(), count))
				.to(topicsConfig.analytics().courierDeliveries(),
				    Produced.with(
							WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(10).toMillis()),
						    Serdes.Long()));

		return performanceTable;
	}
}
