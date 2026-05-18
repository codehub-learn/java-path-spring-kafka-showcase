package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.enums.OrderStatus;
import gr.codelearn.spring.kafka.domain.event.OrderStatusUpdatedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/*
 * T2 — Level 1 (Stateless): demonstrates KStream.split().branch() for conditional routing.
 *
 * Routes OrderStatusUpdatedEvent into three named branches:
 *   - in-progress  → ACCEPTED, PREPARING, READY, PICKED_UP (logged only)
 *   - completed    → DELIVERED                             → food.analytics.orders-completed
 *   - failed       → DELIVERY_FAILED                       → food.analytics.orders-failed
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderStatusBranchTopology {

	private static final Set<OrderStatus> IN_PROGRESS_STATUSES = EnumSet.of(
			OrderStatus.ACCEPTED, OrderStatus.PREPARING, OrderStatus.READY, OrderStatus.PICKED_UP);

	private final TopicsConfig topicsConfig;

	@Bean
	public Map<String, KStream<String, OrderStatusUpdatedEvent>> orderStatusBranches(
			StreamsBuilder streamsBuilder) {

		var statusSerde = new JsonSerde<>(OrderStatusUpdatedEvent.class);

		KStream<String, OrderStatusUpdatedEvent> statusStream = streamsBuilder
				.stream(topicsConfig.statusUpdated(),
				        Consumed.with(Serdes.String(), statusSerde));

		return statusStream
				.split(Named.as("branch-"))
				.branch(
						(key, event) -> IN_PROGRESS_STATUSES.contains(event.newStatus()),
						Branched.withConsumer(
								s -> s.peek((k, e) -> log.debug("In-progress orderId={} status={}",
								                                e.orderId(), e.newStatus())),
								"in-progress"))
				.branch(
						(key, event) -> event.newStatus() == OrderStatus.DELIVERED,
						Branched.withConsumer(
								s -> {
									s.peek((k, e) -> log.info("Completed orderId={}", e.orderId()));
									s.to(topicsConfig.analytics().ordersCompleted(),
									     Produced.with(Serdes.String(), statusSerde));
								},
								"completed"))
				.branch(
						(key, event) -> event.newStatus() == OrderStatus.DELIVERY_FAILED,
						Branched.withConsumer(
								s -> {
									s.peek((k, e) -> log.info("Delivery failed orderId={}", e.orderId()));
									s.to(topicsConfig.analytics().ordersFailed(),
									     Produced.with(Serdes.String(), statusSerde));
								},
								"failed"))
				.defaultBranch(Branched.as("other"));
	}
}
