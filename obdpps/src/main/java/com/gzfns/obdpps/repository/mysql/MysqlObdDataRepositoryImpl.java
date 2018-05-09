package com.gzfns.obdpps.repository.mysql;

import com.gzfns.obdpps.entity.ObdDataEntity;
import com.gzfns.obdpps.util.HashUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MysqlObdDataRepositoryImpl extends AbstractMysqlRepository {

	private static final Logger logger = LoggerFactory.getLogger(MysqlObdDataRepositoryImpl.class);

	private static final String SQL_INSERT_HEADER = "INSERT INTO ";
	private static final String SQL_INSERT_TAIL = "(`Imei`, `ReportTime`,`ReceiveTime`,`Latitude`,`Longitude`,`BLatitude`,`BLongitude`,`TotalMileage`,`Speed`,`IsValid`,`Orientation`,`LocationType`,`ExternalVoltage`) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);";

	private static final String batchSqlFields = "(Imei,ReportTime,ReceiveTime,Latitude,Longitude,BLatitude,BLongitude,TotalMileage,Speed,IsValid,Orientation,LocationType,ExternalVoltage) VALUES";

	private static final String batchSqlTail = " ON DUPLICATE KEY UPDATE ReportTime=VALUES(ReportTime)," +
			"ReceiveTime=VALUES(ReceiveTime)," +
			"Latitude=VALUES(Latitude)," +
			"Longitude=VALUES(Longitude)," +
			"BLatitude=VALUES(BLatitude)," +
			"BLongitude =VALUES(BLongitude)," +
			"TotalMileage=VALUES(TotalMileage)," +
			"Speed=VALUES(Speed)," +
			"IsValid=VALUES(IsValid)," +
			"Orientation=VALUES(Orientation)," +
			"LocationType=VALUES(LocationType)," +
			"ExternalVoltage=VALUES(ExternalVoltage);";

	public MysqlObdDataRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> batchInsertOrUpdateCurrent(List<ObdDataEntity> obdDataList, int batchSize){
		Future<Boolean> result = Future.future();

		int totalCount = obdDataList.size();
		if(totalCount == 0){
			result.complete(true);
		}
		else {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append(SQL_INSERT_HEADER);
			sqlBuilder.append("device_obd_data");
			sqlBuilder.append(batchSqlFields);

			int batchTimes = totalCount / batchSize;
			int modNum = totalCount % batchSize;
			logger.trace("current, total count: " + totalCount + ", batch size: " + batchSize + ", batch times: " + batchTimes + ", mod num: " + modNum);

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
						sql1 = buildObdDataValuesSql(obdDataList.get(startIndex), true);
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
						sql1 = buildObdDataValuesSql(obdDataList.get(startIndex), false);
						sqlBuilder.append(sql1);
					}
				}

				CompositeFuture.all(futures).setHandler(timesRes -> {
					if(timesRes.succeeded()){
						logger.trace("All batch times insert or update current table successfully.");
						timesFut.complete(true);
					}
					else {
						logger.error("Failed All batch times insert or update current table.", timesRes.cause());
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
						sql2 = buildObdDataValuesSql(obdDataList.get(startIndex), true);
					}
					else
					{
						sql2 = buildObdDataValuesSql(obdDataList.get(startIndex), false);
					}
					sqlBuilder.append(sql2);
				}

				sqlBuilder.append(batchSqlTail);

				executeInsertOrUpdateSql(sqlBuilder).setHandler(res -> {
					if(res.succeeded()){
						logger.trace("batch mod insert or update current table successfully.");
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
					logger.trace("All batch insert or update current table successfully.");
					result.complete(true);
				}
				else {
					result.fail(composeRes.cause());
				}
			});
		}

		return result;
	}

	public Future<Boolean> batchInsertHistory(Map<String, List<ObdDataEntity>> obdDataListMap, int batchSize) {
		Future<Boolean> result = Future.future();

		List<Future> futures = new ArrayList<>();
		for (String key : obdDataListMap.keySet()) {
			List<ObdDataEntity> listOfKeyHistoryTable = obdDataListMap.get(key);
			Future<Boolean> batchFut = batchInsertHistoryToSingleTable(key, listOfKeyHistoryTable, batchSize);
			futures.add(batchFut);
		}

		CompositeFuture.all(futures).setHandler(batchHisRes -> {
			if(batchHisRes.succeeded()){
				logger.trace("All batch insert history table successfully.");
				result.complete(true);
			}
			else {
				logger.error("Failed All batch insert history table.", batchHisRes.cause());
				result.fail(batchHisRes.cause());
			}
		});

		return result;
	}


	private Future<Boolean> batchInsertHistoryToSingleTable(String tableName, List<ObdDataEntity> obdDataList, int batchSize){
		Future<Boolean> result = Future.future();

		int totalCount = obdDataList.size();
		if(totalCount == 0){
			result.complete(true);
		}
		else {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append(SQL_INSERT_HEADER);
			sqlBuilder.append(tableName);
			sqlBuilder.append(batchSqlFields);

			int batchTimes = totalCount / batchSize;
			int modNum = totalCount % batchSize;
			logger.trace("history, total count: " + totalCount + ", batch size: " + batchSize + ", batch times: " + batchTimes + ", mod num: " + modNum);

			Future<Boolean> timesFut = Future.future();
			int startIndex = 0;

			//batchSize的倍数条
			if (batchTimes > 0)
			{
				int timesTotalCount = batchSize * batchTimes;
				String sqlTimes = "";

				List<Future> futures = new ArrayList<>();

				for (startIndex = 0; startIndex < timesTotalCount; startIndex++)
				{
					if ((startIndex + 1) % batchSize == 0)
					{
						sqlTimes = buildObdDataValuesSql(obdDataList.get(startIndex), true);
						sqlBuilder.append(sqlTimes);

						Future<Boolean> exeInsOrUpdateFut = executeInsertOrUpdateSql(sqlBuilder);
						futures.add(exeInsOrUpdateFut);

						sqlBuilder = new StringBuilder();
						sqlBuilder.append(SQL_INSERT_HEADER);
						sqlBuilder.append(tableName);
						sqlBuilder.append(batchSqlFields);
					}
					else
					{
						sqlTimes = buildObdDataValuesSql(obdDataList.get(startIndex), false);
						sqlBuilder.append(sqlTimes);
					}
				}

				CompositeFuture.all(futures).setHandler(timesRes -> {
					if(timesRes.succeeded()){
						logger.trace("All batch times insert single history table successfully. tableName: {}", tableName);
						timesFut.complete(true);
					}
					else {
						logger.error("Failed All batch times insert single history table.", timesRes.cause());
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
				String sqlMod = "";
				startIndex = batchSize * batchTimes;
				for (; startIndex < totalCount; startIndex++)
				{
					if (startIndex == totalCount - 1)
					{
						sqlMod = buildObdDataValuesSql(obdDataList.get(startIndex), true);
					}
					else
					{
						sqlMod = buildObdDataValuesSql(obdDataList.get(startIndex), false);
					}
					sqlBuilder.append(sqlMod);
				}

				executeInsertOrUpdateSql(sqlBuilder).setHandler(res -> {
					if(res.succeeded()){
						logger.trace("All batch mod insert single history table successfully. tableName: " + tableName);
						modFut.complete(true);
					}
					else {
						logger.error("Failed All batch mod insert single history table.", res.cause());
						modFut.fail(res.cause());
					}
				});
			}
			else {
				modFut.complete(true);
			}

			CompositeFuture.all(timesFut, modFut).setHandler(composeRes -> {
				if(composeRes.succeeded()){
					logger.trace("All batch insert single history table successfully. tableName: " + tableName);
					result.complete(true);
				}
				else {
					logger.error("Failed All batch insert single history table.", composeRes.cause());
					result.fail(composeRes.cause());
				}
			});
		}

		return result;
	}

	private String buildObdDataValuesSql(ObdDataEntity data, boolean isLast)
	{
		String sqlValues = "('%s','%s','%s','%s','%s','%s','%s',%.2f,%.2f,%d,%.2f,%d,%d)";

		if (!isLast)
		{
			sqlValues += ",";
		}

		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

		sqlValues = String.format(sqlValues, data.getImei(),
				dateFormatter.format(data.getReportTime()),
				dateFormatter.format(data.getReceiveTime()),
				data.getLatitude(),
				data.getLongitude(),
				data.getbLatitude(),
				data.getbLongitude(),
				data.getTotalMileage(),
				data.getSpeed(),
				data.getIsValid(),
				data.getOrientation(),
				data.getLocationType(),
				data.getExternalVoltage());

		return sqlValues;
	}

	private Future<Boolean> executeInsertOrUpdateSql(StringBuilder sqlBuilder){
		Future<Boolean> result = Future.future();

		logger.trace("execute sql: " + sqlBuilder.toString());

		jdbcClient.getConnection(connHandler(result, connection -> {
			logger.trace("Obd data Get mysql connection successfully.");

			connection.update(sqlBuilder.toString(), r -> {
				if (r.succeeded()) {
					logger.trace("Execute obd data sql successfully.");
					result.complete(true);
				} else {
					logger.error("execute obd data sql error.", r.cause());
					result.fail(r.cause());
				}

				connection.close(closeres -> {
					if(closeres.succeeded()){
						logger.trace("Obd data execute insert or update sql connection closed successfully.");
					}
					else {
						logger.error("Failed close Obd data execute insert or update sql connection.", closeres.cause());
					}
				});
			});
		}));

		return result;
	}

	public Future<Boolean> insert(ObdDataEntity obdDataEntity) {
		Future<Boolean> result = Future.future();
		try{
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			final String reportTime = format.format(obdDataEntity.getReportTime());
			final String receiveTime = format.format(obdDataEntity.getReceiveTime());

			jdbcClient.getConnection(connHandler(result, connection -> {
				connection.updateWithParams(SQL_INSERT_HEADER + getTableName(obdDataEntity.getImei()) + SQL_INSERT_TAIL, new JsonArray()
						.add(obdDataEntity.getImei())
						.add(reportTime)
						.add(receiveTime)
						.add(obdDataEntity.getLatitude())
						.add(obdDataEntity.getLongitude())
						.add(obdDataEntity.getbLatitude())
						.add(obdDataEntity.getbLongitude())
						.add(obdDataEntity.getTotalMileage())
						.add(obdDataEntity.getSpeed())
						.add(obdDataEntity.getIsValid())
						.add(obdDataEntity.getOrientation())
						.add(obdDataEntity.getLocationType())
						.add(obdDataEntity.getExternalVoltage()), r -> {
					if (r.failed()) {
						result.fail(r.cause());
					} else {
						result.complete(true);
					}
					connection.close(closeres -> {
						if(closeres.succeeded()){
							logger.trace("Obd data insert connection closed successfully.");
						}
						else {
							logger.error("Failed close Obd data insert connection.", closeres.cause());
						}
					});
				});
			}));
		}
		catch (Exception ex)
		{
			logger.error(ex.getMessage(), ex);
			result.fail(ex.getCause());
		}

		return result;
	}

	private static String getTableName(String imei){
		String tablename = "device_obd_data_" + HashUtil.getHashValue(imei) % 100;
		logger.info("obddata table is: " + tablename + ",imei is: " + imei);
		return tablename;
	}
}
