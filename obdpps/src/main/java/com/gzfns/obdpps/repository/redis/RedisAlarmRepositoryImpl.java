package com.gzfns.obdpps.repository.redis;

import com.gzfns.obdpps.entity.DeviceAlarmEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisAlarmRepositoryImpl extends AbstractRedisRepository {

	private static final Logger logger = LoggerFactory.getLogger(RedisAlarmRepositoryImpl.class);

	public RedisAlarmRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(DeviceAlarmEntity alarmEntity) {
		Future<Boolean> result = Future.future();
		final String encoded = Json.encodePrettily(alarmEntity);
		logger.trace("key: " + "obd_alarm_" + alarmEntity.getImei() + "_" + alarmEntity.getAlarmCode() + ", value: " + encoded);
		redis.set("obd_alarm_" + alarmEntity.getImei() + "_" + alarmEntity.getAlarmCode(), encoded, res -> {
			if(res.succeeded()){
				logger.trace("insert alarm to redis successfully.");
				result.complete(true);
			}
			else {
				logger.error("Failed insert alarm to redis. imei: " + alarmEntity.getImei()
						+ ", alarmCode: " + alarmEntity.getAlarmCode(), res.cause());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});
		return result;
	}

	public Future<DeviceAlarmEntity> get(DeviceAlarmEntity where) {
		Future<DeviceAlarmEntity> result = Future.future();

		String key = "obd_alarm_" + where.getImei() + "_" + where.getAlarmCode();
		redis.get(key, res -> {
			if(res.succeeded()){
				if(res.result() != null) {
					logger.trace("get alarm: {}", res.result());
					result.complete(Json.decodeValue(res.result(), DeviceAlarmEntity.class));
				}
				else {
					logger.trace("get alarm: null.");
					result.complete(null);
				}
			}
			else {
				logger.error("Failed get alarm from redis, imei: " + where.getImei(), res.cause());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});

		return result;
	}
}
