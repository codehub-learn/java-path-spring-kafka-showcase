package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.model.OrderCompletionEvent;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.Set;

/*
 * T7 — Level 4 (Stream-Stream Join): demonstrates KStream.join() with JoinWindows.
 *
 * Joins the placed stream with the terminal-status stream on orderId to produce
 * an OrderCompletionEvent that captures the full lifecycle duration in seconds.
 *
 * Both streams must be rekeyed to orderId before joining because:
 *   - food.orders.placed is keyed by restaurantId (set by fos-producer/fos-generator)
 *   - food.orders.status-updated is keyed by orderId
 * Rekeying the placed stream triggers a repartition; the status stream is already correct.
 *
 * JoinWindows.ofTimeDifferenceWithNoGrace(30 min) means Kafka Streams looks for a
 * matching status event within 30 minutes of the placed event (in event-time).
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderCompletionTopology {

	private static final Set<OrderStatus> TERMINAL_STATUSES =
			EnumSet.of(OrderStatus.DELIVERED, OrderStatus.DELIVERY_FAILED);

	private final TopicsConfig topicsConfig;

	@Bean
	public KStream<String, OrderCompletionEvent> orderCompletionStream(StreamsBuilder streamsBuilder) {
		var placedSerde = new JsonSerde<>(OrderPlacedEvent.class);
		var statusSerde = new JsonSerde<>(OrderStatusUpdatedEvent.class);
		var completionSerde = new JsonSerde<>(OrderCompletionEvent.class);

		// Rekey placed events from restaurantId → orderId (triggers repartition)
		KStream<String, OrderPlacedEvent> placedByOrderId = streamsBuilder
				.stream(topicsConfig.placed(), Consumed.with(Serdes.String(), placedSerde))
				.selectKey((key, event) -> event.orderId());

		// Keep only terminal (DELIVERED / DELIVERY_FAILED) status events
		KStream<String, OrderStatusUpdatedEvent> terminalStatus = streamsBuilder
				.stream(topicsConfig.statusUpdated(), Consumed.with(Serdes.String(), statusSerde))
				.filter((key, event) -> TERMINAL_STATUSES.contains(event.newStatus()));

		KStream<String, OrderCompletionEvent> completions = placedByOrderId.join(
				terminalStatus,
				(placed, status) -> {
					long durationSeconds = Duration
							.between(placed.placedAt().toInstant(ZoneOffset.UTC),
							         status.updatedAt().toInstant(ZoneOffset.UTC))
							.abs()
							.getSeconds();
					return new OrderCompletionEvent(
							placed.orderId(),
							placed.restaurantId(),
							placed.customerId(),
							status.newStatus().name(),
							durationSeconds);
				},
				JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30)),
				StreamJoined.with(Serdes.String(), placedSerde, statusSerde));

		completions
				.peek((orderId, event) ->
							  log.info("Order {} completed as {} in {}s",
						               event.orderId(), event.outcome(), event.durationSeconds()))
				.to(topicsConfig.analytics().orderDuration(),
				    Produced.with(Serdes.String(), completionSerde));

		return completions;
	}
}
