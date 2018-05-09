package com.gzfns.obdpps.repository.redis;

import com.gzfns.obdpps.entity.ObdDataEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisObdDataRepositoryImpl extends AbstractRedisRepository {

	private static final Logger logger = LoggerFactory.getLogger(RedisObdDataRepositoryImpl.class);

	public RedisObdDataRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(ObdDataEntity obdDataEntity) {
		Future<Boolean> result = Future.future();
		final String encoded = Json.encodePrettily(obdDataEntity);
		logger.trace("key: " + "obd_obddata_" + obdDataEntity.getImei()+ " value: " + encoded);
		redis.set("obd_obddata_" + obdDataEntity.getImei(), encoded, res -> {
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
