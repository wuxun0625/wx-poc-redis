package com.wx.poc.wxpocredis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WxPocRedisApplication implements CommandLineRunner {

	@Autowired
	private RedisPoc stringTypePoc;


	public static void main(String[] args) {
		SpringApplication.run(WxPocRedisApplication.class, args);
	}


	@Override
	public void run(String... args) throws Exception {
		// Redis String type GET/SET
		stringTypePoc.redisStringGetSet();
		// Redis String type Inc/Dec
		stringTypePoc.redisStringIncDec();
		// Redis String type JSON/Object
		stringTypePoc.redisStringJsonObject();
		// Redis Hash type Get/Set
		stringTypePoc.redisHashGetSet();
		// Redis List type Get/Set/LPOP/RPOP
		stringTypePoc.redisListGetSetLpopRpop();
	}




}
