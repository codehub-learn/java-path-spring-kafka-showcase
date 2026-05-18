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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.time.Duration;

/*
 * T9 — Level 6 (Session Windows): demonstrates SessionWindows with a custom aggregate.
 *
 * Groups orders by customerId and accumulates spend within activity sessions.
 * A session closes after 5 minutes of inactivity for that customer. Back-to-back
 * orders within the gap are merged into one session; orders after the gap open a new one.
 *
 * Session aggregation requires THREE lambdas (vs two for tumbling/hopping):
 *   1. initializer   — zero value for a brand-new session
 *   2. aggregator    — adds an event to an open session
 *   3. sessionMerger — merges two separate sessions when a late event bridges the gap
 *                      (session "stitching"); absent from tumbling/hopping aggregations
 *
 * selectKey() from orderId → customerId triggers an implicit repartition.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class CustomerSpendTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KTable<Windowed<String>, BigDecimal> customerSpendTable(StreamsBuilder streamsBuilder) {
		KTable<Windowed<String>, BigDecimal> spendTable = streamsBuilder
				.stream(topicsConfig.placed(),
				        Consumed.with(Serdes.String(), new JsonSerde<>(OrderPlacedEvent.class)))
				// Rekeying from restaurantId → customerId (triggers repartition)
				.selectKey((key, event) -> event.customerId())
				.groupByKey()
				.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
				.aggregate(
						() -> BigDecimal.ZERO,
						(customerId, event, sessionTotal) -> sessionTotal.add(event.totalAmount()),
						// Session merger: called when a late event stitches two sessions together
						(customerId, left, right) -> left.add(right),
						Materialized
								.<String, BigDecimal, SessionStore<Bytes, byte[]>>as("customer-spend-store")
								.withKeySerde(Serdes.String())
								.withValueSerde(new JsonSerde<>(BigDecimal.class)));

		spendTable
				.toStream()
				.peek((windowedKey, sessionTotal) ->
							  log.info("Customer {} session spend [{}–{}]: {}",
						               windowedKey.key(),
						               windowedKey.window().startTime(),
						               windowedKey.window().endTime(),
						               sessionTotal))
				.to(topicsConfig.analytics().customerSpend(),
				    Produced.with(WindowedSerdes.sessionWindowedSerdeFrom(String.class),
				                  new JsonSerde<>(BigDecimal.class)));

		return spendTable;
	}
}
