package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.TwitterToKafkaSearchConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ComponentScan(basePackages = "come.microservices.demo")
@Slf4j
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "false",matchIfMissing = true)
public class TwitterKafkaStreamRunnerImpl implements StreamRunner {

    private final TwitterToKafkaSearchConfigData searchContextConfiguration;
    private final TwitterKafkaStatusListener  twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunnerImpl(TwitterToKafkaSearchConfigData searchContextConfiguration,
                                        TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.searchContextConfiguration = searchContextConfiguration;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();


    }

    private void addFilter() {
        String[] keywords = searchContextConfiguration.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        log.info("Started Filtering twitter stream of key words {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public  void shutdown(){
        if(twitterStream!=null){
            log.info("closing twitter stream");
            twitterStream.shutdown();
        }
    }
}
