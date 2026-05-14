package gr.codelearn.spring.kafka.consumer;

import gr.codelearn.spring.kafka.consumer.config.TopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(TopicsConfig.class)
public class FosConsumerApplication {
	static void main(String[] args) {
		SpringApplication.run(FosConsumerApplication.class, args);
	}
}
