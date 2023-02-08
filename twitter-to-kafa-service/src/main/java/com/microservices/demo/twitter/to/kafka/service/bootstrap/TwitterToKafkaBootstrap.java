package com.microservices.demo.twitter.to.kafka.service.bootstrap;

import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import twitter4j.TwitterException;

@Component
@Slf4j
public class TwitterToKafkaBootstrap implements CommandLineRunner {

    private  final StreamRunner streamRunner;

    public TwitterToKafkaBootstrap(StreamRunner streamRunner) {
        this.streamRunner = streamRunner;
    }


    @Override
    public void run(String... args) throws Exception {
        loadDate();
    }

    private void loadDate() throws TwitterException {
        streamRunner.start();
    }
}
