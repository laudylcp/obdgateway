package com.gzfns.obdpps.repository.mysql;

import com.gzfns.obdpps.entity.DeviceAlarmEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

public class MysqlAlarmRepositoryImpl extends AbstractMysqlRepository {

	private static final Logger logger = LoggerFactory.getLogger(MysqlAlarmRepositoryImpl.class);

	private static final String SQL_INSERT = "INSERT INTO `device_alarm` " +
			"(`Imei`, `AlarmCode`, `AlarmTime`, `AlarmThresholdValue`, `AlarmStatus`) VALUES (?, ?, ?, ?, ?)";

	private static final String SQL_GetBy_Where = "select * from `device_alarm` " +
			"where Imei=? and AlarmCode=? and AlarmStatus=? order by AlarmTime desc limit 1;";

	private static final String UPDATE_By_Id = "update device_alarm set AlarmStatus=?, AlarmRestoreTime=? where Id=?;";

	public MysqlAlarmRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(DeviceAlarmEntity alarmEntity) {
		Future<Boolean> result = Future.future();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String AlarmTime = format.format(alarmEntity.getAlarmTime());
		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.updateWithParams(SQL_INSERT, new JsonArray()
					.add(alarmEntity.getImei())
					.add(alarmEntity.getAlarmCode())
					.add(AlarmTime)
					.add(alarmEntity.getAlarmThresholdValue())
		            .add(alarmEntity.getAlarmStatus()), r -> {
				if (r.failed()) {
					logger.error("Failed insert alarm to mysql.", r.cause());
					result.fail(r.cause());
				} else {
					logger.trace("insert alarm to mysql successfully.");
					result.complete(true);
				}
				connection.close(closeres -> {
					if(closeres.succeeded()){
						logger.trace("Device alarm insert connection closed successfully.");
					}
					else {
						logger.error("Failed close device alarm insert connection.", closeres.cause());
					}
				});
			});
		}));
		return result;
	}

	public Future<DeviceAlarmEntity> get(DeviceAlarmEntity where) {
		Future<DeviceAlarmEntity> result = Future.future();

		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.queryWithParams(SQL_GetBy_Where, new JsonArray()
					.add(where.getImei())
					.add(where.getAlarmCode())
					.add(where.getAlarmStatus()), r -> {
				if(r.succeeded()){
					if(r.result().getNumRows() > 0) {
						logger.trace("get first alarm row: " + r.result().getRows().get(0).encodePrettily());
						JsonObject firstRow = r.result().getRows().get(0);
						result.complete(Json.decodeValue(firstRow.toBuffer(), DeviceAlarmEntity.class));
					}
					else {
						result.complete(null);
					}
				}
				else {
					result.fail(r.cause());
				}
				connection.close(closeres -> {
					if(closeres.succeeded()){
						logger.trace("Device alarm get connection closed successfully.");
					}
					else {
						logger.error("Failed close device alarm get connection.", closeres.cause());
					}
				});
			});
		}));

		return result;
	}

	public Future<Boolean> update(DeviceAlarmEntity entity) {
		Future<Boolean> result = Future.future();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String alarmRestoreTime = format.format(entity.getAlarmRestoreTime());

		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.updateWithParams(UPDATE_By_Id, new JsonArray()
					.add(entity.getAlarmStatus())
					.add(alarmRestoreTime)
					.add(entity.getId()), r -> {
				if(r.succeeded()){
					result.complete(true);
				}
				else {
					result.fail(r.cause());
				}

				connection.close(closeres -> {
					if(closeres.succeeded()){
						logger.trace("Device alarm update connection closed successfully.");
					}
					else {
						logger.error("Failed close device alarm update connection.", closeres.cause());
					}
				});
			});
		}));

		return result;
	}
}
