package gr.codelearn.spring.kafka.generator;

import gr.codelearn.spring.kafka.generator.config.GeneratorProperties;
import gr.codelearn.spring.kafka.generator.config.TopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({TopicsConfig.class, GeneratorProperties.class})
public class FosGeneratorApplication {

	static void main(String[] args) {
		SpringApplication.run(FosGeneratorApplication.class, args);
	}
}
