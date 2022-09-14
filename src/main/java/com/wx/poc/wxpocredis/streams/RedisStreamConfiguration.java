package com.wx.poc.wxpocredis.streams;

import com.wx.poc.wxpocredis.dto.Heroes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class RedisStreamConfiguration {

    @Autowired
    private RedisConnectionFactory connFactory;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, ObjectRecord<String, Heroes>> streamMessageListenerContainer() {
        System.out.println("Start Redis streams listenter.");
        AtomicInteger index = new AtomicInteger();
        int processors = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(processors, processors, 0, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(), r -> {
            Thread thread = new Thread(r);
            thread.setName("consumer-" + index.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, Heroes>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .batchSize(1)
                        .executor(executor)
                        .pollTimeout(Duration.ofSeconds(0))
                        .targetType(Heroes.class)
                        .build();

        StreamMessageListenerContainer<String, ObjectRecord<String, Heroes>> streamMessageListenerContainer =
                StreamMessageListenerContainer.create(connFactory,options);


        Heroes hero = new Heroes();
        hero.setName("name");
        hero.setRealName("realName");
        hero.setSuperPower("superPower");

        ObjectRecord<String, Heroes> record = StreamRecords.newRecord().in("test_streams_02").ofObject(hero).withId(RecordId.autoGenerate());
        redisTemplate.opsForStream().add(record);
        try {
            redisTemplate.opsForStream().createGroup("test_streams_02","group1");
        } catch (Exception e) {

        }
        ConsumeListener consumeListener = new ConsumeListener();
        consumeListener.setRedisTemplate(redisTemplate);
        streamMessageListenerContainer.receive(Consumer.from("group1","consumer1"),
                StreamOffset.create("test_streams_02", ReadOffset.lastConsumed()), consumeListener);
        streamMessageListenerContainer.receive(Consumer.from("group1","consumer2"),
                StreamOffset.create("test_streams_02", ReadOffset.lastConsumed()), consumeListener);

        return streamMessageListenerContainer;
    }


}
