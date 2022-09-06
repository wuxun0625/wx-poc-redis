package com.wx.poc.wxpocredis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wx.poc.wxpocredis.dto.Heroes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class RedisPoc {
    @Autowired
    private StringRedisTemplate redisTemplate;

    // Redis String type GET/SET
    public void redisStringGetSet() {
        System.out.println("==========redisStringGetSet==============");
        redisTemplate.opsForValue().set("test_string_01","Hello "+System.nanoTime());
        System.out.println(redisTemplate.opsForValue().get("test_string_01"));
    }

    // Redis String type Inc/Dec
    public void redisStringIncDec() {
        System.out.println("==========redisStringIncDec==============");
        redisTemplate.opsForValue().set("test_string_02","0");
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
        redisTemplate.opsForValue().set("test_string_03",om.writeValueAsString(hero));
        Heroes heroRead = om.readValue(redisTemplate.opsForValue().get("test_string_03"),Heroes.class);
        System.out.println(heroRead);
    }

    // Redis Hash type Get/Set
    public void redisHashGetSet() {
        System.out.println("==========redisHashGetSet==============");
        Map<String,String> heroes = new HashMap<String,String>();
        heroes.put("name","Batman");
        heroes.put("realName","Bruce Wayne");
        heroes.put("superPower","I'm RICH!!!");
        redisTemplate.opsForHash().putAll("test_hash_01",heroes);
        Set<Object> keys = redisTemplate.opsForHash().keys("test_hash_01");
        for(Object key: keys) {
            System.out.println(key.toString()+ ": " + redisTemplate.opsForHash().get("test_hash_01",key));
        }
    }

    // Redis List type Get/Set/LPOP/RPOP
    public void redisListGetSetLpopRpop() {
        System.out.println("==========redisListGetSetLpopRpop==============");
        redisTemplate.opsForList().leftPush("test_list_01","row1");
        redisTemplate.opsForList().leftPush("test_list_01","row2");
        redisTemplate.opsForList().rightPush("test_list_01","row3");
        System.out.println("Left pop: "+redisTemplate.opsForList().leftPop("test_list_01"));
        System.out.println("Left pop: "+redisTemplate.opsForList().leftPop("test_list_01"));
        System.out.println("Left pop: "+redisTemplate.opsForList().leftPop("test_list_01"));
        redisTemplate.opsForList().leftPush("test_list_01","row1");
        redisTemplate.opsForList().leftPush("test_list_01","row2");
        redisTemplate.opsForList().rightPush("test_list_01","row3");
        System.out.println("Right pop: "+redisTemplate.opsForList().rightPop("test_list_01"));
        System.out.println("Right pop: "+redisTemplate.opsForList().rightPop("test_list_01"));
        System.out.println("Right pop: "+redisTemplate.opsForList().rightPop("test_list_01"));
    }

}
