package gr.codelearn.spring.kafka.streams;

import gr.codelearn.spring.kafka.streams.config.TopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(TopicsConfig.class)
public class FosStreamsApplication {

	static void main(String[] args) {
		SpringApplication.run(FosStreamsApplication.class, args);
	}
}
