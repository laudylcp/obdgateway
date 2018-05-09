package com.gzfns.obdpps.repository.redis;

import com.gzfns.obdpps.entity.DeviceVehicleEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisDeviceVehicleRepositoryImpl extends AbstractRedisRepository {

	private static final Logger logger = LoggerFactory.getLogger(RedisDeviceVehicleRepositoryImpl.class);

	public RedisDeviceVehicleRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(DeviceVehicleEntity deviceVehicleEntity) {
		Future<Boolean> result = Future.future();
		final String encoded = Json.encodePrettily(deviceVehicleEntity);
		logger.trace("key: " + "obd_vin_" + deviceVehicleEntity.getImei()  + " value: " + encoded);
		redis.set("obd_vin_" + deviceVehicleEntity.getImei(), encoded, res -> {
			if(res.succeeded()){
				logger.trace("update Vin to " + "obd_vin_" + deviceVehicleEntity.getImei() + " successfully.");
				result.complete(true);
			}
			else {
				logger.error("Failed update Vin to " + "obd_vin_" + deviceVehicleEntity.getImei());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});
		return result;
	}


}
