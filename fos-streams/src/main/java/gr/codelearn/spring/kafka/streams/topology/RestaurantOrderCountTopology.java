package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*
 * T3 — Level 2 (Stateful, no windows): demonstrates groupBy() → count() → KTable.
 *
 * Maintains a running total of all-time orders per restaurant. The count never resets.
 * The KTable is backed by a local state store and its changelog is compacted (log-compaction
 * ensures consumers always see the latest count per restaurant key).
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RestaurantOrderCountTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KTable<String, Long> restaurantOrderCountTable(StreamsBuilder streamsBuilder) {
		KTable<String, Long> countTable = streamsBuilder
				.stream(topicsConfig.placed(),
				        Consumed.with(Serdes.String(), new JsonSerde<>(OrderPlacedEvent.class)))
				.groupBy(
						(key, event) -> event.restaurantId(),
						Grouped.with("restaurant-order-count-grouped", Serdes.String(),
						             new JsonSerde<>(OrderPlacedEvent.class)))
				.count(Materialized.as("restaurant-order-count-store"));

		countTable
				.toStream()
				.peek((restaurantId, count) ->
							  log.info("Restaurant {} total orders: {}", restaurantId, count))
				.to(topicsConfig.analytics().restaurantOrderCount(),
				    Produced.with(Serdes.String(), Serdes.Long()));

		return countTable;
	}
}
