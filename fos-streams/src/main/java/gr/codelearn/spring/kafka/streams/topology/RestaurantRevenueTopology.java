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

import java.math.BigDecimal;
import java.time.Duration;

/*
 * T5 — Level 3 (Windowed): demonstrates aggregate() vs count() in a windowed context.
 *
 * Sums totalAmount per restaurant in non-overlapping 5-minute tumbling windows.
 * Key difference from T4: aggregate() requires an initializer and an adder lambda,
 * whereas count() is a specialisation that increments a Long internally.
 * BigDecimal is serialized via JsonSerde backed by Jackson ObjectMapper.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RestaurantRevenueTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KTable<Windowed<String>, BigDecimal> restaurantRevenueTable(StreamsBuilder streamsBuilder) {
		KTable<Windowed<String>, BigDecimal> revenueTable = streamsBuilder
				.stream(topicsConfig.placed(),
				        Consumed.with(Serdes.String(), new JsonSerde<>(OrderPlacedEvent.class)))
				.groupBy(
						(key, event) -> event.restaurantId(),
						Grouped.with("restaurant-revenue-grouped", Serdes.String(),
						             new JsonSerde<>(OrderPlacedEvent.class)))
				.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
				.aggregate(
						() -> BigDecimal.ZERO,
						(restaurantId, event, acc) -> acc.add(event.totalAmount()),
						Materialized
								.<String, BigDecimal, WindowStore<Bytes, byte[]>>as("restaurant-revenue-store")
								.withKeySerde(Serdes.String())
								.withValueSerde(new JsonSerde<>(BigDecimal.class)));

		revenueTable
				.toStream()
				.peek((windowedKey, revenue) ->
							  log.info("Restaurant {} revenue [{}–{}]: {}",
						               windowedKey.key(),
						               windowedKey.window().startTime(),
						               windowedKey.window().endTime(),
						               revenue))
				.to(topicsConfig.analytics().restaurantRevenue(),
				    Produced.with(
							WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(5).toMillis()),
						    new JsonSerde<>(BigDecimal.class)));

		return revenueTable;
	}
}
