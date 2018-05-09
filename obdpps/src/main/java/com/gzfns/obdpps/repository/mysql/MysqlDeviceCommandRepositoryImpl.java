package com.gzfns.obdpps.repository.mysql;

import com.gzfns.obdpps.entity.DeviceCommandEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

public class MysqlDeviceCommandRepositoryImpl extends AbstractMysqlRepository {

	private static final Logger logger = LoggerFactory.getLogger(MysqlDeviceCommandRepositoryImpl.class);

	private static final String SQL_GETBY = "SELECT * FROM device_command WHERE Imei = ? and CommandType = ?;";

	private static final String SQL_COUNTBY = "SELECT COUNT(*) FROM device_command WHERE Imei = ? and CommandType = ?;";

	private static final String SQL_INSERT = "INSERT INTO `device_command` " +
			"(Imei,CommandType,CommandContent,AddTime,SendTime,IsSend,IsReply,ReplyTime) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

	private static final String SQL_UPDATE = "UPDATE `device_command` " +
			"SET CommandContent=?,AddTime=?,SendTime=?,IsSend=?,IsReply=?,ReplyTime=? " +
			"WHERE Imei=? and CommandType=?;";

	private static final String SQL_UPDATE_REPLY = "UPDATE `device_command` " +
			"SET IsReply=?,ReplyTime=? " +
			"WHERE Imei=? and CommandType=?;";


	public MysqlDeviceCommandRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(DeviceCommandEntity deviceCommandEntity) {
		Future<Boolean> result = Future.future();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String addTime = format.format(deviceCommandEntity.getAddTime());
		final String sendTime = format.format(deviceCommandEntity.getSendTime());
		final String replyTime = format.format(deviceCommandEntity.getReplyTime());

		getCountBy(deviceCommandEntity).setHandler(res -> {
			if(res.succeeded()) {
				int existCount = res.result();
				if(existCount > 0) {//已经存在imei+commandType相同的记录
					jdbcClient.getConnection(connHandler(result, connection -> {
						connection.updateWithParams(SQL_UPDATE, new JsonArray()
								.add(deviceCommandEntity.getCommandContent())
								.add(addTime)
								.add(sendTime)
								.add(deviceCommandEntity.getIsSend())
								.add(deviceCommandEntity.getIsReply())
								.add(replyTime)
								.add(deviceCommandEntity.getImei())
								.add(deviceCommandEntity.getCommandType()), r -> {
							if (r.failed()) {
								result.fail(r.cause());
							} else {
								result.complete(true);
							}
							connection.close(closeRes -> {
								if(closeRes.succeeded()){
									logger.trace("Device command insert connection closed successfully.");
								}
								else {
									logger.error("Failed close device command insert connection.", closeRes.cause());
								}
							});
						});
					}));
				}
				else {
					jdbcClient.getConnection(connHandler(result, connection -> {
						connection.updateWithParams(SQL_INSERT, new JsonArray()
								.add(deviceCommandEntity.getImei())
								.add(deviceCommandEntity.getCommandType())
								.add(deviceCommandEntity.getCommandContent())
								.add(addTime)
								.add(sendTime)
								.add(deviceCommandEntity.getIsSend())
								.add(deviceCommandEntity.getIsReply())
								.add(replyTime), r -> {
							if (r.succeeded()) {
								result.complete(true);

							} else {
								result.fail(r.cause());
							}
							connection.close(closeRes -> {
								if(closeRes.succeeded()){
									logger.trace("Device command insert connection closed successfully.");
								}
								else {
									logger.error("Failed close device command insert connection.", closeRes.cause());
								}
							});
						});
					}));
				}
			}
			else {
				result.fail(res.cause());
			}
		});

		return result;
	}

	public Future<Integer> getCountBy(DeviceCommandEntity deviceCommandEntity) {
		Future<Integer> result = Future.future();

		jdbcClient.getConnection(connHandler(result, connection ->
			connection.queryWithParams(SQL_COUNTBY, new JsonArray().add(deviceCommandEntity.getImei())
					.add(deviceCommandEntity.getCommandType()),r -> {
				if (r.succeeded()) {
					int existsCount = r.result().getResults().get(0).getInteger(0);
					logger.trace("Get device command records count success. count: " + existsCount);
					result.complete(existsCount);
				} else {
					logger.error("Get device command records count failed.", r.cause());
					result.fail(r.cause());
				}
				connection.close(closeRes -> {
					if(closeRes.succeeded()){
						logger.trace("Device command getCountBy connection closed successfully.");
					}
					else {
						logger.error("Failed close device command getCountBy connection.", closeRes.cause());
					}
				});
			})));

		return result;
	}

	public Future<DeviceCommandEntity> get(DeviceCommandEntity deviceCommandEntity) {
		Future<DeviceCommandEntity> result = Future.future();

		jdbcClient.getConnection(connHandler(result, connection ->
			connection.queryWithParams(SQL_GETBY, new JsonArray()
					.add(deviceCommandEntity.getImei())
					.add(deviceCommandEntity.getCommandType()),r -> {
				if (r.succeeded()) {
					JsonObject record = r.result().getResults().get(0).getJsonObject(0);
					logger.trace("Get device command record success. record: " + record.encodePrettily());
					DeviceCommandEntity old = Json.decodeValue(record.toBuffer(), DeviceCommandEntity.class);
					result.complete(old);
				} else {
					logger.error("Get device command record failed.", r.cause());
					result.fail(r.cause());
				}
				connection.close(closeRes -> {
					if(closeRes.succeeded()){
						logger.trace("Device command get connection closed successfully.");
					}
					else {
						logger.error("Failed close device command get connection.", closeRes.cause());
					}
				});
			})));

		return result;
	}

	public Future<Boolean> update(DeviceCommandEntity deviceCommandEntity) {
		Future<Boolean> result = Future.future();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String replyTime = format.format(deviceCommandEntity.getReplyTime());
		jdbcClient.getConnection(connHandler(result, connection ->
				connection.queryWithParams(SQL_UPDATE_REPLY, new JsonArray()
						.add(deviceCommandEntity.getIsReply())
						.add(replyTime)
						.add(deviceCommandEntity.getImei())
						.add(deviceCommandEntity.getCommandType()),r -> {
					if (r.succeeded()) {
						logger.trace("update device command record success.");
						result.complete(true);
					} else {
						logger.error("update device command record failed.", r.cause());
						result.fail(r.cause());
					}
					connection.close(closeRes -> {
						if(closeRes.succeeded()){
							logger.trace("Device command update connection closed successfully.");
						}
						else {
							logger.error("Failed close device command update connection.", closeRes.cause());
						}
					});
				})));

		return result;
	}
}
