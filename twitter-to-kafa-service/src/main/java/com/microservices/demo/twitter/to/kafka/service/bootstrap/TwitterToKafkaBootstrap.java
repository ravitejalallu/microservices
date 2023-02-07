package com.microservices.demo.twitter.to.kafka.service.bootstrap;

import com.microservices.demo.twitter.to.kafka.service.Configuration.SearchContextConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterToKafkaBootstrap implements CommandLineRunner {

    private  final SearchContextConfiguration contextConfiguration;

    public TwitterToKafkaBootstrap(SearchContextConfiguration contextConfiguration) {
        this.contextConfiguration = contextConfiguration;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info(contextConfiguration.getWelcomeMessage());
        loadDate();
    }

    private void loadDate() {
        contextConfiguration.getTwitterKeywords().forEach(log::info);
    }
}
