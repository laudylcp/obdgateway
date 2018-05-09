package com.gzfns.obdpps.repository.mysql;

import com.gzfns.obdpps.entity.HeartBeatEntity;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class MysqlHeartBeatRepositoryImpl extends AbstractMysqlRepository {

	private static final Logger logger = LoggerFactory.getLogger(MysqlHeartBeatRepositoryImpl.class);

	private static final String SQL_INSERT_HEADER = "INSERT INTO ";

	private static final String batchSqlFields = "(`Imei`, `HeartbeatTime`, `HadOfflineAlarm`) VALUES";

	private static final String batchSqlTail = " ON DUPLICATE KEY UPDATE HeartbeatTime=VALUES(HeartbeatTime), HadOfflineAlarm=VALUES(HadOfflineAlarm);";

	private static final String SQL_INSERT = "INSERT INTO `device_heartbeat` " +
			"(`Imei`, `HeartbeatTime`, `HadOfflineAlarm`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE HeartbeatTime=VALUES(HeartbeatTime), HadOfflineAlarm=VALUES(HadOfflineAlarm);";

	private static final String SQL_SELECT_BY_HADOFFLINEALARM = "SELECT * FROM device_heartbeat WHERE HadOfflineAlarm=?";

	private static final String UPDATE_By_Id = "update device_heartbeat set HadOfflineAlarm=? where Imei=?;";

	public MysqlHeartBeatRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> batchInsertOrUpdateHeartbeat(List<HeartBeatEntity> heartbeatList, int batchSize){
		Future<Boolean> result = Future.future();

		int totalCount = heartbeatList.size();
		if(totalCount == 0){
			result.complete(true);
		}
		else {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append(SQL_INSERT_HEADER);
			sqlBuilder.append("device_heartbeat");
			sqlBuilder.append(batchSqlFields);

			int batchTimes = totalCount / batchSize;
			int modNum = totalCount % batchSize;
			logger.trace("heartbeat, total count: " + totalCount + ", batch size: " + batchSize + ", batch times: " + batchTimes + ", mod num: " + modNum);

			Future<Boolean> timesFut = Future.future();
			int startIndex = 0;

			//batchSize的倍数条
			if (batchTimes > 0)
			{
				int timesTotalCount = batchSize * batchTimes;
				String sql1 = "";

				List<Future> futures = new ArrayList<>();

				for (startIndex = 0; startIndex < timesTotalCount; startIndex++)
				{
					if ((startIndex + 1) % batchSize == 0)
					{
						sql1 = buildHeartbeatValuesSql(heartbeatList.get(startIndex), true);
						sqlBuilder.append(sql1);
						sqlBuilder.append(batchSqlTail);

						Future<Boolean> exeInsOrUpdateFut = executeInsertOrUpdateSql(sqlBuilder);
						futures.add(exeInsOrUpdateFut);

						sqlBuilder = new StringBuilder();
						sqlBuilder.append(SQL_INSERT_HEADER);
						sqlBuilder.append("device_obd_data");
						sqlBuilder.append(batchSqlFields);
					}
					else
					{
						sql1 = buildHeartbeatValuesSql(heartbeatList.get(startIndex), false);
						sqlBuilder.append(sql1);
					}
				}

				CompositeFuture.all(futures).setHandler(timesRes -> {
					if(timesRes.succeeded()){
						logger.trace("All batch times insert or update heartbeat table successfully.");
						timesFut.complete(true);
					}
					else {
						logger.error("Failed All batch times insert or update heartbeat table.", timesRes.cause());
						timesFut.fail(timesRes.cause());
					}
				});
			}
			else {
				timesFut.complete(true);
			}

			Future<Boolean> modFut = Future.future();
			if (modNum > 0)
			{
				String sql2 = "";
				startIndex = batchSize * batchTimes;
				for (; startIndex < totalCount; startIndex++)
				{
					if (startIndex == totalCount - 1)
					{
						sql2 = buildHeartbeatValuesSql(heartbeatList.get(startIndex), true);
					}
					else
					{
						sql2 = buildHeartbeatValuesSql(heartbeatList.get(startIndex), false);
					}
					sqlBuilder.append(sql2);
				}

				sqlBuilder.append(batchSqlTail);

				executeInsertOrUpdateSql(sqlBuilder).setHandler(res -> {
					if(res.succeeded()){
						logger.trace("batch mod insert or update heartbeat table successfully.");
						modFut.complete(true);
					}
					else {
						modFut.fail(res.cause());
					}
				});
			}
			else {
				modFut.complete(true);
			}

			CompositeFuture.all(timesFut, modFut).setHandler(composeRes -> {
				if(composeRes.succeeded()){
					logger.trace("All batch insert or update heartbeat table successfully.");
					result.complete(true);
				}
				else {
					logger.error("Failed All batch insert or update heartbeat table.", composeRes.cause());
					result.fail(composeRes.cause());
				}
			});
		}

		return result;
	}

	private String buildHeartbeatValuesSql(HeartBeatEntity data, boolean isLast)
	{
		String sqlValues = "('%s','%s',%d)";

		if (!isLast)
		{
			sqlValues += ",";
		}

		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

		sqlValues = String.format(sqlValues, data.getImei(),
				dateFormatter.format(data.getHeartbeatTime()),
				data.getHadOfflineAlarm());

		return sqlValues;
	}

	private Future<Boolean> executeInsertOrUpdateSql(StringBuilder sqlBuilder){
		Future<Boolean> result = Future.future();

		logger.trace("execute heartbeat sql: " + sqlBuilder.toString());

		jdbcClient.getConnection(connHandler(result, connection -> {
			logger.trace("Get heartbeat mysql connection successfully.");

			connection.update(sqlBuilder.toString(), r -> {
				if (r.succeeded()) {
					logger.trace("Execute heartbeat sql successfully.");
					result.complete(true);
				} else {
					logger.error("execute heartbeat sql error.", r.cause());
					result.fail(r.cause());
				}

				connection.close(connClosed -> {
					if(connClosed.succeeded()){
						logger.trace("Heartbeat mysql connection has closed successfully.");
					}
					else {
						logger.error("Failed close mysql connection.", connClosed.cause());
					}
				});
			});
		}));

		return result;
	}

	public Future<Boolean> insert(HeartBeatEntity heartBeatEntity) {
		Future<Boolean> result = Future.future();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String HeartBeatTime = format.format(heartBeatEntity.getHeartbeatTime());
		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.updateWithParams(SQL_INSERT, new JsonArray()
					.add(heartBeatEntity.getImei())
					.add(HeartBeatTime)
					.add(heartBeatEntity.getHadOfflineAlarm()), r -> {
				if (r.failed()) {
					logger.error("Failed insert heartbeat " + heartBeatEntity.getImei(), r.cause());
					result.fail(r.cause());
				} else {
					logger.trace("insert heartbeat " + heartBeatEntity.getImei() + " successfully.");
					result.complete(true);
				}
				connection.close(closeRes -> {
					if(closeRes.succeeded()){
						logger.trace("Heartbeat insert connection closed successfully.");
					}
					else {
						logger.error("Failed close Heartbeat insert connection.", closeRes.cause());
					}
				});
			});
		}));
		return result;
	}

	public Future<List<HeartBeatEntity>> getHeartbeatListByHadOfflineAlarm(HeartBeatEntity where) {
		Future<List<HeartBeatEntity>> result = Future.future();

		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.queryWithParams(SQL_SELECT_BY_HADOFFLINEALARM, new JsonArray()
					.add(where.getHadOfflineAlarm()), r -> {
				if (r.failed()) {
					result.fail(r.cause());
				} else {
					ResultSet resultSet = r.result();
					List<HeartBeatEntity> heartBeatEntityList = new ArrayList<>();
					for (JsonObject json : resultSet.getRows()) {
						HeartBeatEntity heartBeatEntity = Json.decodeValue(json.toBuffer(), HeartBeatEntity.class);
						heartBeatEntityList.add(heartBeatEntity);
					}
					logger.trace("getHeartbeatListByHadOfflineAlarm successfully. list size: " + heartBeatEntityList.size());
					result.complete(heartBeatEntityList);
				}
				connection.close(closeRes -> {
					if(closeRes.succeeded()){
						logger.trace("Heartbeat getHeartbeatListByHadOfflineAlarm connection closed successfully.");
					}
					else {
						logger.error("Failed close Heartbeat getHeartbeatListByHadOfflineAlarm connection.", closeRes.cause());
					}
				});
			});
		}));

		return result;
	}

	public Future<Boolean> updateByImei(HeartBeatEntity entity) {
		Future<Boolean> result = Future.future();

		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.updateWithParams(UPDATE_By_Id, new JsonArray()
					.add(entity.getHadOfflineAlarm())
					.add(entity.getImei()), r -> {
				if(r.succeeded()){
					result.complete(true);
				}
				else {
					result.fail(r.cause());
				}

				connection.close(closeRes -> {
					if(closeRes.succeeded()){
						logger.trace("Heartbeat updateByImei connection closed successfully.");
					}
					else {
						logger.error("Failed close Heartbeat updateByImei connection.", closeRes.cause());
					}
				});
			});
		}));

		return result;
	}

}
