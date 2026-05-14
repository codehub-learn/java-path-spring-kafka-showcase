package gr.codelearn.spring.kafka.producer;

import gr.codelearn.spring.kafka.producer.config.TopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(TopicsConfig.class)
public class FosProducerApplication {
	static void main(String[] args) {
		SpringApplication.run(FosProducerApplication.class, args);
	}
}
