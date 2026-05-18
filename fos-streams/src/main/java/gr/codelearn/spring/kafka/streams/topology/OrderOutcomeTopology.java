package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderCancelledEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.model.OutcomeEvent;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/*
 * T8 — Level 5 (Multi-Stream Merge): demonstrates KStream.merge() across 3 input streams.
 *
 * Merges three sources of terminal order outcomes into a single stream, then counts
 * outcomes by type within 5-minute tumbling windows:
 *   - DELIVERED    → from food.orders.status-updated filtered for DELIVERED
 *   - FAILED       → from food.orders.status-updated filtered for DELIVERY_FAILED
 *   - CANCELLED    → from food.orders.cancelled
 *
 * merge() is a simple union — records from all three sub-streams flow through the same
 * downstream pipeline. The merged stream is then grouped by outcome type and counted.
 * This topology shows how to combine events of different shapes (two different record
 * types) into a single common model (OutcomeEvent) before merging.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderOutcomeTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KStream<String, OutcomeEvent> orderOutcomeStream(StreamsBuilder streamsBuilder) {
		var statusSerde = new JsonSerde<>(OrderStatusUpdatedEvent.class);
		var cancelledSerde = new JsonSerde<>(OrderCancelledEvent.class);
		var outcomeSerde = new JsonSerde<>(OutcomeEvent.class);

		KStream<String, OutcomeEvent> deliveredStream = streamsBuilder
				.stream(topicsConfig.statusUpdated(), Consumed.with(Serdes.String(), statusSerde))
				.filter((key, event) -> event.newStatus() == OrderStatus.DELIVERED)
				.mapValues(event -> new OutcomeEvent(event.orderId(), "DELIVERED"));

		KStream<String, OutcomeEvent> failedStream = streamsBuilder
				.stream(topicsConfig.statusUpdated(), Consumed.with(Serdes.String(), statusSerde))
				.filter((key, event) -> event.newStatus() == OrderStatus.DELIVERY_FAILED)
				.mapValues(event -> new OutcomeEvent(event.orderId(), "DELIVERY_FAILED"));

		KStream<String, OutcomeEvent> cancelledStream = streamsBuilder
				.stream(topicsConfig.cancelled(), Consumed.with(Serdes.String(), cancelledSerde))
				.mapValues(event -> new OutcomeEvent(event.orderId(), "CANCELLED"));

		// Merge all three outcome streams into one unified pipeline
		KStream<String, OutcomeEvent> merged = deliveredStream.merge(failedStream).merge(cancelledStream);

		merged
				.groupBy(
						(key, event) -> event.outcome(),
						Grouped.with("order-outcome-grouped", Serdes.String(), outcomeSerde))
				.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
				.count(Materialized
							   .<String, Long, WindowStore<Bytes, byte[]>>as("order-outcome-store")
						       .withKeySerde(Serdes.String())
						       .withValueSerde(Serdes.Long()))
				.toStream()
				.peek((windowedKey, count) ->
							  log.info("Outcome {} in window [{}–{}]: {}",
						               windowedKey.key(),
						               windowedKey.window().startTime(),
						               windowedKey.window().endTime(),
						               count))
				.to(topicsConfig.analytics().orderOutcomes(),
				    Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(5).toMillis()),
				                  Serdes.Long()));

		return merged;
	}
}
