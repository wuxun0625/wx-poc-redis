package com.wx.poc.wxpocredis.streams;

import com.wx.poc.wxpocredis.dto.Heroes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;

public class ConsumeListener implements StreamListener<String, ObjectRecord<String, Heroes>> {

    private StringRedisTemplate redisTemplate;

    public void setRedisTemplate(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void onMessage(ObjectRecord<String, Heroes> message) {
        String stream = message.getStream();
        RecordId recordId = message.getId();
        Heroes hero = message.getValue();
        System.out.println(Thread.currentThread().getName() + " Received message, key: " + stream + " record id: " + recordId.getValue() + " body: " + hero.toString());
        redisTemplate.opsForStream().acknowledge(stream, "group1",recordId.getValue());
//        redisTemplate.opsForStream().delete(stream, recordId.getValue());
    }
}
