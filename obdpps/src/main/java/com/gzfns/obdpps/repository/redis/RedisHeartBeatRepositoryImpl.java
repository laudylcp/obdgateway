package com.gzfns.obdpps.repository.redis;

import com.gzfns.obdpps.entity.HeartBeatEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisHeartBeatRepositoryImpl extends AbstractRedisRepository {

	private static final Logger logger = LoggerFactory.getLogger(RedisHeartBeatRepositoryImpl.class);

	public RedisHeartBeatRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(HeartBeatEntity heartBeatEntity) {
		Future<Boolean> result = Future.future();
		final String encoded = Json.encodePrettily(heartBeatEntity);
		logger.trace("key: " + "obd_heartbeat_" + heartBeatEntity.getImei() + " value: " + encoded);
		redis.set("obd_heartbeat_" + heartBeatEntity.getImei(), encoded, res -> {
			if(res.succeeded()){
				result.complete(true);
			}
			else {
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});
		return result;
	}

}
