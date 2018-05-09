package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.DeviceVehicleEntity;
import com.gzfns.obdpps.repository.mysql.MysqlDeviceVehicleRepositoryImpl;
import com.gzfns.obdpps.repository.redis.RedisDeviceVehicleRepositoryImpl;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceVehicleServiceImpl implements IDeviceVehicleService {

    private static final Logger logger = LoggerFactory.getLogger(DeviceVehicleServiceImpl.class);

    private static Vertx vertx;

    private RedisDeviceVehicleRepositoryImpl redisDeviceVehicleRepository;

    private MysqlDeviceVehicleRepositoryImpl mysqlDeviceVehicleRepository;

    public DeviceVehicleServiceImpl(Vertx vertx){
        this.vertx = vertx;
        this.redisDeviceVehicleRepository = new RedisDeviceVehicleRepositoryImpl(vertx);
        this.mysqlDeviceVehicleRepository = new MysqlDeviceVehicleRepositoryImpl(vertx);
    }

    @Override
    public Future<Boolean> insert(DeviceVehicleEntity deviceVehicleEntity) {
        Future<Boolean> result = Future.future();

        Future<Boolean> redisFut = redisDeviceVehicleRepository.insert(deviceVehicleEntity);

        Future<Boolean> mysqlFut = mysqlDeviceVehicleRepository.insert(deviceVehicleEntity);

        CompositeFuture.all(redisFut, mysqlFut).setHandler(res -> {
            if(res.succeeded()){
                logger.trace("insert device vehicle entity to redis and mysql successfully.");
                result.complete(true);
            }
            else {
                logger.error("Failed insert device vehicle entity to redis or mysql.", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }
}
