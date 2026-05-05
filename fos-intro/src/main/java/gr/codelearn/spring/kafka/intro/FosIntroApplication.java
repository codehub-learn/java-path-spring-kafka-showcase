package gr.codelearn.spring.kafka.intro;

import gr.codelearn.spring.kafka.intro.config.TopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(TopicsConfig.class)
public class FosIntroApplication {
	static void main(String[] args) {
		SpringApplication.run(FosIntroApplication.class, args);
	}
}
