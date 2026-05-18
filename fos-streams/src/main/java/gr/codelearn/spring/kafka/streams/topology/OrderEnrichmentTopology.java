package gr.codelearn.spring.kafka.streams.topology;

import gr.codelearn.spring.kafka.domain.event.OrderPlacedEvent;
import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import gr.codelearn.spring.kafka.streams.model.EnrichedOrderEvent;
import gr.codelearn.spring.kafka.streams.model.PriceTier;
import gr.codelearn.spring.kafka.streams.serde.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*
 * T1 — Level 1 (Stateless): demonstrates mapValues(), peek(), and filter() on a single KStream.
 *
 * Reads OrderPlacedEvent, enriches it with a PriceTier (LOW / MEDIUM / HIGH),
 * logs every event, and writes the result to the order-enriched topic.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderEnrichmentTopology {

	private final TopicsConfig topicsConfig;

	@Bean
	public KStream<String, EnrichedOrderEvent> orderEnrichmentStream(StreamsBuilder streamsBuilder) {
		KStream<String, EnrichedOrderEvent> enriched = streamsBuilder
				.stream(topicsConfig.placed(),
				        Consumed.with(Serdes.String(), new JsonSerde<>(OrderPlacedEvent.class)))
				.mapValues(event -> new EnrichedOrderEvent(
						event.orderId(),
						event.restaurantId(),
						event.customerId(),
						event.totalAmount(),
						PriceTier.from(event.totalAmount()),
						event.placedAt()))
				.peek((key, event) ->
							  log.debug("Enriched orderId={} restaurant={} amount={} tier={}",
						                event.orderId(), event.restaurantId(),
						                event.totalAmount(), event.priceTier()));

		enriched.to(topicsConfig.analytics().orderEnriched(),
		            Produced.with(Serdes.String(), new JsonSerde<>(EnrichedOrderEvent.class)));

		return enriched;
	}
}
