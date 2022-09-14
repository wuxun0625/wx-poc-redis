package com.wx.poc.wxpocredis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.connection.stream.RecordId;

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
		// Redis Set type Get/Set
		stringTypePoc.redisSetGetSet();
		// Redis Set type collection operations
		stringTypePoc.redisSetOps();
		// Redis ordered Set type
		stringTypePoc.redisSortedSet();
		// Redis hyperLogLogs type
		stringTypePoc.redisHyperLogLogs();
		// Redis bitmaps type
		stringTypePoc.redisBitMaps();
		// Redis geospatial type
		stringTypePoc.redisGeoSpatial();



		// Redis streams add message
		RecordId id1 = stringTypePoc.redisAddStreamsMsg("test_streams_01","Superman", "Clark.Kent", "Super power");
		RecordId id2 = stringTypePoc.redisAddStreamsMsg("test_streams_01","Batman", "Bruce.Wayne", "I'm RICH!!!");
		RecordId id3 = stringTypePoc.redisAddStreamsMsg("test_streams_01","奥特曼", "乡秀树", "泽斯蒂姆光线");
		// Redis streams get messages by range
		stringTypePoc.redisReadStreamsMsgRange(id2.toString(),id3.toString());
		// Redis streams get messages by read
		stringTypePoc.redisReadStreamsMsg(id3.toString());
		// Add message for test streams listener
		for(int i=0;i<100;i++) {
			stringTypePoc.redisAddStreamsMsg("test_streams_02","奥特曼" + i, "乡秀树" + i, "泽斯蒂姆光线" + i);
		}
		Thread.sleep(100000);
	}






}
