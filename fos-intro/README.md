# fos-intro

Demonstrates Spring Kafka fundamentals:

- **AdminClient** — programmatic topic creation via `NewTopic` beans and `KafkaAdmin`
- **Producer** — explicit `ProducerFactory` + `KafkaTemplate<String, String>` configuration
- **Consumer** — explicit `ConsumerFactory` + `ConcurrentKafkaListenerContainerFactory`, `@KafkaListener`
- **Topic name externalisation** — `TopicsConfig` record + `@ConfigurationProperties`, referenced via SpEL in
  `@KafkaListener`

On startup `IntroRunner` sends two messages; `HelloMessageConsumer` logs each one received.

## Running locally

Start the Docker Kafka cluster first:

```bash
docker compose up -d
```

Then run the module:

```bash
mvn spring-boot:run -pl fos-intro
# or
java -jar fos-intro/target/fos-intro-0.0.1-SNAPSHOT.jar
```

## Running tests

```bash
mvn test -pl fos-intro
```

Tests use `@EmbeddedKafka` — no external broker needed.
