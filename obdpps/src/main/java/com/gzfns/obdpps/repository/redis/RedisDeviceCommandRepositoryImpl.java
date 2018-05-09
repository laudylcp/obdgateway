package com.gzfns.obdpps.repository.redis;

import com.gzfns.obdpps.entity.DeviceCommandEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RedisDeviceCommandRepositoryImpl extends AbstractRedisRepository {

	private static final Logger logger = LoggerFactory.getLogger(RedisDeviceCommandRepositoryImpl.class);

	public RedisDeviceCommandRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<List<DeviceCommandEntity>> getList(DeviceCommandEntity where) {
		Future<List<DeviceCommandEntity>> result = Future.future();

		redis.keys("obd_devicecommand_" + where.getImei(), res -> {
			if(res.succeeded()){
				JsonArray commandArray = res.result();
				List objectList = commandArray.getList();
				List<DeviceCommandEntity> commandList = new ArrayList<>();
				for (Object command : objectList) {
					DeviceCommandEntity commandEntity = Json.decodeValue(JsonObject.mapFrom(command).toBuffer(), DeviceCommandEntity.class);
					commandList.add(commandEntity);
				}
				logger.trace("Get command list from redis successfully. imei: {}", where.getImei());
				result.complete(commandList);
			}
			else {
				logger.error("failed get device command list from redis.", res.cause());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});

		return result;
	}

	public Future<Boolean> insert(DeviceCommandEntity deviceCommandEntity) {
		Future<Boolean> result = Future.future();
		final String encoded = Json.encodePrettily(deviceCommandEntity);
		logger.trace("key: " + "obd_devicecommand_" + deviceCommandEntity.getImei() + "_" + deviceCommandEntity.getCommandType() + " value: " + encoded);
		redis.set("obd_devicecommand_" + deviceCommandEntity.getImei() + "_" + deviceCommandEntity.getCommandType(), encoded, res -> {
			if(res.succeeded()){
				logger.trace("insert device command to redis successfully.");
				result.complete(true);
			}
			else {
				logger.error("failed insert device command to redis.", res.cause());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});
		return result;
	}

	public Future<DeviceCommandEntity> get(DeviceCommandEntity deviceCommandEntity) {
		Future<DeviceCommandEntity> result = Future.future();

		redis.get("obd_devicecommand_" + deviceCommandEntity.getImei() + "_" + deviceCommandEntity.getCommandType(), res -> {
			if(res.succeeded()){
				String value = res.result();
				DeviceCommandEntity entity = Json.decodeValue(value, DeviceCommandEntity.class);
				logger.trace("get device command from redis successfully.");
				result.complete(entity);
			}
			else {
				logger.error("failed get device command from redis.", res.cause());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});

		return result;
	}

	public Future<Boolean> delete(DeviceCommandEntity deviceCommandEntity) {
		Future<Boolean> result = Future.future();

		redis.del("obd_devicecommand_" + deviceCommandEntity.getImei() + "_" + deviceCommandEntity.getCommandType(), res -> {
			if(res.succeeded()){
				logger.trace("delete device command from redis successfully.");
				result.complete(true);
			}
			else {
				logger.error("failed delete device command from redis.", res.cause());
				result.fail(res.cause());
			}

			redis.close(closeRes -> {
				logger.trace("redis closed.");
			});
		});

		return result;
	}
}
