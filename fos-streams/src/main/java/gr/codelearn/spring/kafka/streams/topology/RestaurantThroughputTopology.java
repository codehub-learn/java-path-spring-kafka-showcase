package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/*
 * T4 — Level 3 (Windowed): demonstrates groupBy() → windowedBy(TumblingWindows) → count().
 *
 * Counts orders per restaurant within non-overlapping 1-minute tumbling windows.
 * Unlike T3's all-time count, each window resets — useful for spike detection.
 * Output key is a Windowed<String> serialized with the built-in TimeWindowedSerde.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RestaurantThroughputTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KTable<Windowed<String>, Long> restaurantThroughputTable(StreamsBuilder streamsBuilder) {
		KTable<Windowed<String>, Long> throughputTable = streamsBuilder
				.stream(topicsConfig.placed(),
				        Consumed.with(Serdes.String(), new JsonSerde<>(OrderPlacedEvent.class)))
				.groupBy(
						(key, event) -> event.restaurantId(),
						Grouped.with("restaurant-throughput-grouped", Serdes.String(),
						             new JsonSerde<>(OrderPlacedEvent.class)))
				.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
				.count(Materialized
							   .<String, Long, WindowStore<Bytes, byte[]>>as("restaurant-throughput-store")
						       .withKeySerde(Serdes.String())
						       .withValueSerde(Serdes.Long()));

		throughputTable
				.toStream()
				.peek((windowedKey, count) ->
							  log.info("Restaurant {} throughput [{}–{}]: {}",
						               windowedKey.key(),
						               windowedKey.window().startTime(),
						               windowedKey.window().endTime(),
						               count))
				.to(topicsConfig.analytics().restaurantThroughput(),
				    Produced.with(
							WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(1).toMillis()),
						    Serdes.Long()));

		return throughputTable;
	}
}
