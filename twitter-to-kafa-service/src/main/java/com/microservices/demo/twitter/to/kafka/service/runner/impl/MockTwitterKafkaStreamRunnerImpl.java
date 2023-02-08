package com.microservices.demo.twitter.to.kafka.service.runner.impl;


import com.microservices.demo.TwitterToKafkaSearchConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Slf4j
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "true")
@ComponentScan(basePackages = "com.microservices.demo")
public class MockTwitterKafkaStreamRunnerImpl implements StreamRunner {

    private final TwitterToKafkaSearchConfigData searchContextConfiguration ;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[]{
            "in",
            "iaculis",
            "nunc",
            "sed",
            "augue",
            "lacus",
            "viverra",
            "vitae",
            "congue",
            "eu",
            "consequat",
            "ac",
            "felis",
            "donec",
            "et",
            "odio",
            "pellentesque",
            "diam",
            "volutpat",
            "commodo"
    };

    private static final String TWEET_AS_RAW_JSON = "{"+
            "\"created_at\":\"{0}\","+
            "\"id\":\"{1}\","+
            "\"text\":\"{2}\","+
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private  static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockTwitterKafkaStreamRunnerImpl(TwitterToKafkaSearchConfigData searchContextConfiguration, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.searchContextConfiguration = searchContextConfiguration;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }


    @Override
    public void start() throws TwitterException {
       String[] keyWords = searchContextConfiguration.getTwitterKeywords().toArray(new String[0]);
       int minTweetLength = searchContextConfiguration.getMockMinTweetLength();
       int maxTweetLength = searchContextConfiguration.getMockMaxTweetLength();
       long sleepTimeMs = searchContextConfiguration.getMockSleepMs();
       log.info("Starting mock filtering stream for keywords {}",keyWords);
        simulateTwitterStream(keyWords, minTweetLength, maxTweetLength, sleepTimeMs);

    }

    private void simulateTwitterStream(String[] keyWords, int minTweetLength, int maxTweetLength, long sleepTimeMs) {

        Executors.newSingleThreadExecutor().submit(()->{
            try {
                while(true){
                    String formattedTweetAsRawJson = getFormattedTweet(keyWords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e) {
                log.error("Error in Creating twitter status ",e);
            }
        });

    }

    private String getFormattedTweet(String[] keyWords, int minTweetLength, int maxTweetLength) {
        String[] params =  new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keyWords,minTweetLength,maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJasonWithParams(params);
    }

    private String formatTweetAsJasonWithParams(String[] params) {
        String tweet = TWEET_AS_RAW_JSON;
        for(int i = 0; i< params.length; i++){
            tweet = tweet.replace("{" + i +"}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keyWords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength-minTweetLength+1)+minTweetLength;
        return constructRandomTweet(keyWords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keyWords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i< tweetLength; i++){
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength /2){
                tweet.append(keyWords[RANDOM.nextInt(keyWords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }


    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaException("Error while sleeping while waiting for new tweets");
        }
    }
}
