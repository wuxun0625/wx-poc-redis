package com.wx.poc.wxpocredis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wx.poc.wxpocredis.dto.Heroes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

@Service
public class RedisPoc {
    @Autowired
    private StringRedisTemplate redisTemplate;

    // Redis String type GET/SET
    public void redisStringGetSet() {
        System.out.println("==========redisStringGetSet==============");
        redisTemplate.opsForValue().set("test_string_01", "Hello " + System.nanoTime());
        System.out.println(redisTemplate.opsForValue().get("test_string_01"));
    }

    // Redis String type Inc/Dec
    public void redisStringIncDec() {
        System.out.println("==========redisStringIncDec==============");
        redisTemplate.opsForValue().set("test_string_02", "0");
        System.out.println(redisTemplate.opsForValue().get("test_string_02"));
        redisTemplate.opsForValue().increment("test_string_02");
        System.out.println(redisTemplate.opsForValue().get("test_string_02"));
        redisTemplate.opsForValue().decrement("test_string_02");
        System.out.println(redisTemplate.opsForValue().get("test_string_02"));
    }

    // Redis String type JSON/Object
    public void redisStringJsonObject() throws JsonProcessingException {
        System.out.println("==========redisStringJsonObject==============");
        Heroes hero = new Heroes();
        hero.setName("Batman");
        hero.setRealName("Bruce Wayne");
        hero.setSuperPower("I'm RICH!!!");
        ObjectMapper om = new ObjectMapper();
        redisTemplate.opsForValue().set("test_string_03", om.writeValueAsString(hero));
        Heroes heroRead = om.readValue(redisTemplate.opsForValue().get("test_string_03"), Heroes.class);
        System.out.println(heroRead);
    }

    // Redis Hash type Get/Set
    public void redisHashGetSet() {
        System.out.println("==========redisHashGetSet==============");
        Map<String, String> heroes = new HashMap<String, String>();
        heroes.put("name", "Batman");
        heroes.put("realName", "Bruce Wayne");
        heroes.put("superPower", "I'm RICH!!!");
        redisTemplate.opsForHash().putAll("test_hash_01", heroes);
        Set<Object> keys = redisTemplate.opsForHash().keys("test_hash_01");
        for (Object key : keys) {
            System.out.println(key.toString() + ": " + redisTemplate.opsForHash().get("test_hash_01", key));
        }
    }

    // Redis List type Get/Set/LPOP/RPOP
    public void redisListGetSetLpopRpop() {
        System.out.println("==========redisListGetSetLpopRpop==============");
        redisTemplate.opsForList().leftPush("test_list_01", "row1");
        redisTemplate.opsForList().leftPush("test_list_01", "row2");
        redisTemplate.opsForList().rightPush("test_list_01", "row3");
        System.out.println("Left pop: " + redisTemplate.opsForList().leftPop("test_list_01"));
        System.out.println("Left pop: " + redisTemplate.opsForList().leftPop("test_list_01"));
        System.out.println("Left pop: " + redisTemplate.opsForList().leftPop("test_list_01"));
        redisTemplate.opsForList().leftPush("test_list_01", "row1");
        redisTemplate.opsForList().leftPush("test_list_01", "row2");
        redisTemplate.opsForList().rightPush("test_list_01", "row3");
        System.out.println("Right pop: " + redisTemplate.opsForList().rightPop("test_list_01"));
        System.out.println("Right pop: " + redisTemplate.opsForList().rightPop("test_list_01"));
        System.out.println("Right pop: " + redisTemplate.opsForList().rightPop("test_list_01"));
    }

    // Redis Set type Get/Set
    public void redisSetGetSet() {
        System.out.println("==========redisSetGetSet==============");
        redisTemplate.opsForSet().add("test_set_01", "row1");
        redisTemplate.opsForSet().add("test_set_01", "row2");
        redisTemplate.opsForSet().add("test_set_01", "row3");
        System.out.println("row3 is member of test_set_01? " + redisTemplate.opsForSet().isMember("test_set_01", "row3"));
        System.out.println(redisTemplate.opsForSet().pop("test_set_01"));
        System.out.println(redisTemplate.opsForSet().pop("test_set_01"));
        System.out.println(redisTemplate.opsForSet().pop("test_set_01"));
    }

    // Redis Set type collection operations
    public void redisSetOps() {
        System.out.println("==========redisSetOps==============");
        redisTemplate.opsForSet().add("test_set_02", "row1");
        redisTemplate.opsForSet().add("test_set_02", "row2");
        redisTemplate.opsForSet().add("test_set_02", "row3");
        redisTemplate.opsForSet().add("test_set_03", "row4");
        redisTemplate.opsForSet().add("test_set_03", "row5");
        redisTemplate.opsForSet().add("test_set_03", "row3");
        System.out.println("集合1: " + redisTemplate.opsForSet().members("test_set_02"));
        System.out.println("集合2: " + redisTemplate.opsForSet().members("test_set_03"));
        System.out.println("交集: " + redisTemplate.opsForSet().intersect("test_set_02", "test_set_03"));
        System.out.println("差集(集合1): " + redisTemplate.opsForSet().difference("test_set_02", "test_set_03"));
        System.out.println("并集: " + redisTemplate.opsForSet().union("test_set_02", "test_set_03"));
    }

    // Redis ordered Set type
    public void redisSortedSet() {
        System.out.println("==========redisSortedSet==============");
        redisTemplate.opsForZSet().add("test_zset_01", "row1", 1D);
        redisTemplate.opsForZSet().add("test_zset_01", "row2", 3D);
        redisTemplate.opsForZSet().add("test_zset_01", "row3", 2D);
        System.out.println("Ordered Set: " + redisTemplate.opsForZSet().range("test_zset_01", 0, redisTemplate.opsForZSet().size("test_zset_01") - 1));
        redisTemplate.opsForZSet().incrementScore("test_zset_01", "row3", 2D);
        System.out.println("Increase row3 score Ordered Set: " + redisTemplate.opsForZSet().range("test_zset_01", 0, redisTemplate.opsForZSet().size("test_zset_01") - 1));

    }

    // Redis hyperLogLogs type
    public void redisHyperLogLogs() {
        System.out.println("==========redisHyperLogLogs==============");
        redisTemplate.opsForHyperLogLog().delete("test_hyperloglogs_01");
        redisTemplate.opsForHyperLogLog().add("test_hyperloglogs_01", "1", "1", "3", "4", "5", "6", "7", "8", "9", "1", "1", "12", "13", "14", "15");
        System.out.println("基数: " + redisTemplate.opsForHyperLogLog().size("test_hyperloglogs_01"));
    }

    // Redis bitmaps type
    public void redisBitMaps() {
        System.out.println("==========redisBitMaps==============");
        redisTemplate.opsForValue().setBit("test_bitmaps_01", 0, true);
        redisTemplate.opsForValue().setBit("test_bitmaps_01", 1, true);
        redisTemplate.opsForValue().setBit("test_bitmaps_01", 2, false);
        redisTemplate.opsForValue().setBit("test_bitmaps_01", 3, true);
        redisTemplate.opsForValue().setBit("test_bitmaps_01", 4, true);

        System.out.println("Bitmap index 0: " + redisTemplate.opsForValue().getBit("test_bitmaps_01", 0));
        System.out.println("Bitmap index 1: " + redisTemplate.opsForValue().getBit("test_bitmaps_01", 1));
        System.out.println("Bitmap index 2: " + redisTemplate.opsForValue().getBit("test_bitmaps_01", 2));
        System.out.println("Bitmap index 3: " + redisTemplate.opsForValue().getBit("test_bitmaps_01", 3));
        System.out.println("Bitmap index 4: " + redisTemplate.opsForValue().getBit("test_bitmaps_01", 4));

        System.out.println("Bitmap true count: " + redisTemplate.execute((RedisCallback<Long>) con -> con.bitCount("test_bitmaps_01".getBytes())));
    }

    // Redis geospatial type
    public void redisGeoSpatial() {
        System.out.println("==========redisGeoSpatial==============");
        redisTemplate.opsForGeo().add("citys", new Point(116, 39), "BeiJing");
        redisTemplate.opsForGeo().add("citys", new Point(117, 39), "TianJin");
        redisTemplate.opsForGeo().add("citys", new Point(118, 39), "TangShan");
        redisTemplate.opsForGeo().add("citys", new Point(121, 31), "ShangHai");
        redisTemplate.opsForGeo().add("citys", new Point(109, 18), "SanYa");

        System.out.println("Beijing: " + redisTemplate.opsForGeo().position("citys", "BeiJing"));
        System.out.println("TianJin: " + redisTemplate.opsForGeo().position("citys", "TianJin"));
        System.out.println("TangShan: " + redisTemplate.opsForGeo().position("citys", "TangShan"));
        System.out.println("ShangHai: " + redisTemplate.opsForGeo().position("citys", "ShangHai"));
        System.out.println("SanYa: " + redisTemplate.opsForGeo().position("citys", "SanYa"));

        System.out.println("Distance between BeiJing and ShangHai: " + redisTemplate.opsForGeo().distance("citys", "BeiJing", "ShangHai"));

        Point center = redisTemplate.opsForGeo().position("citys", "BeiJing").get(0);
        Distance radius = new Distance(1000, Metrics.KILOMETERS);
        Circle within = new Circle(center, radius);
        System.out.println("Citys within 1000KM from BeiJing: " + redisTemplate.opsForGeo().radius("citys", within));
    }

    // Redis add streams data
    public RecordId redisAddStreamsMsg(String key,String name, String realName, String superPower) {
        Heroes hero = new Heroes();
        hero.setName(name);
        hero.setRealName(realName);
        hero.setSuperPower(superPower);

        ObjectRecord<String, Heroes> record = StreamRecords.newRecord().in(key).ofObject(hero).withId(RecordId.autoGenerate());
        RecordId recordId = redisTemplate.opsForStream().add(record);
        System.out.println("Streams returned message ID: " + recordId);
        return recordId;
    }

    // Redis read streams message by range
    public void redisReadStreamsMsgRange(String from, String to) {
        System.out.println("==========redisReadStreamsMsgRange==============");
        Range range = Range.closed(from, to);
        List<ObjectRecord<String, Heroes>> result = redisTemplate.opsForStream().range(Heroes.class, "test_streams_01", range);
        for (ObjectRecord<String, Heroes> rs : result) {
            System.out.println("Message ID: " + rs.getId() + " Message Value: " + rs.getValue().toString());
        }
    }

    public void redisReadStreamsMsg(String from) {
        System.out.println("==========redisReadStreamsMsg==============");
        // Initialize thread pool
        List<ObjectRecord<String, Heroes>> result = redisTemplate.opsForStream().read(
                Heroes.class,
                StreamReadOptions.empty().block(Duration.ofMillis(0)).count(1),
                StreamOffset.create("test_streams_01", ReadOffset.from(from)));
        for (ObjectRecord<String, Heroes> rs : result) {
            System.out.println("Message ID: " + rs.getId() + " Message Value: " + rs.getValue().toString());
        }
    }


}
