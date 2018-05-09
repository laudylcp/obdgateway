package com.gzfns.obdpps.repository.mysql;

import com.gzfns.obdpps.entity.DeviceVehicleEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

public class MysqlDeviceVehicleRepositoryImpl extends AbstractMysqlRepository {

	private static final Logger logger = LoggerFactory.getLogger(MysqlDeviceVehicleRepositoryImpl.class);

	private static final String SQL_INSERT = "INSERT INTO `device_vehicle` " +
			"(`Imei`,`Vin`,`LinkedTime`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE Vin=VALUES(Vin), LinkedTime=VALUES(LinkedTime)";

	private static final String SQL_QUERY = "SELECT count(*) As cnt FROM device_vehicle WHERE Imei = ? and Vin=?";

	public MysqlDeviceVehicleRepositoryImpl(Vertx vertx){
		super(vertx);
	}

	public Future<Boolean> insert(DeviceVehicleEntity deviceVehicleEntity) {
		Future<Boolean> result = Future.future();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final String linkedTime = format.format(deviceVehicleEntity.getLinkedTime());

		jdbcClient.getConnection(connHandler(result, connection -> {
			connection.updateWithParams(SQL_INSERT, new JsonArray()
					.add(deviceVehicleEntity.getImei())
					.add(deviceVehicleEntity.getVin())
					.add(linkedTime),r -> {
				if (r.succeeded()) {
					logger.trace("insert or update vehicle Vin successfully.");
					result.complete(true);
				} else {
					logger.error("failed insert or update vehicle Vin. Imei: " + deviceVehicleEntity.getImei(), r.cause());
					result.fail(r.cause());
				}
				connection.close(closeres -> {
					if(closeres.succeeded()){
						logger.trace("Device vehicle connection closed successfully.");
					}
					else {
						logger.error("Failed close device vehicle connection.", closeres.cause());
					}
				});
			});
		}));

		return result;
	}
}
