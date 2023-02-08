package com.microservices.demo.twitter.to.kafka.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class);
    }
}
