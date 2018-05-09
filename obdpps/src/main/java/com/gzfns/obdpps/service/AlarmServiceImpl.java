package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.DeviceAlarmEntity;
import com.gzfns.obdpps.repository.mysql.MysqlAlarmRepositoryImpl;
import com.gzfns.obdpps.repository.redis.RedisAlarmRepositoryImpl;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AlarmServiceImpl implements IAlarmService {

    private static final Logger logger = LoggerFactory.getLogger(AlarmServiceImpl.class);

    private static Vertx vertx;

    private RedisAlarmRepositoryImpl redisAlarmRepository;

    private MysqlAlarmRepositoryImpl mysqlAlarmRepository;

    public AlarmServiceImpl(Vertx vertx){
        this.vertx = vertx;
        this.redisAlarmRepository = new RedisAlarmRepositoryImpl(vertx);
        this.mysqlAlarmRepository = new MysqlAlarmRepositoryImpl(vertx);
    }

    @Override
    public Future<Boolean> insert(DeviceAlarmEntity deviceAlarmEntity) {
        Future<Boolean> result = Future.future();

        Future<Boolean> redisFut = redisAlarmRepository.insert(deviceAlarmEntity);

        Future<Boolean> mysqlFut = mysqlAlarmRepository.insert(deviceAlarmEntity);

        CompositeFuture.all(redisFut, mysqlFut).setHandler(res -> {
            if(res.succeeded()){
                logger.trace("insert alarm entity to redis and mysql successfully.");
                result.complete(true);
            }
            else {
                logger.error("Failed insert alarm entity to redis or mysql.", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }

    @Override
    public Future<Boolean> updateBy(DeviceAlarmEntity where) {
        Future<Boolean> result = Future.future();

        Future<Boolean> redisFut = Future.future();
        redisAlarmRepository.get(where).setHandler(get -> {
            if(get.succeeded()){
                logger.trace("Get alarm from redis successfully, imei: {}", where.getImei());
                if(get.result() != null) {
                    DeviceAlarmEntity targetEntity = get.result();
                    targetEntity.setAlarmStatus((short)0);
                    targetEntity.setAlarmRestoreTime(new Date());

                    redisAlarmRepository.insert(targetEntity).setHandler(insertRes -> {
                        if (insertRes.succeeded()) {
                            logger.trace("update alarm of redis for restore successfully.");
                            redisFut.complete(insertRes.result());
                        } else {
                            logger.error("failed update alarm of redis for restore.", insertRes.cause());
                            redisFut.fail(insertRes.cause());
                        }
                    });
                }
                else {
                    redisFut.complete(false);
                }
            }
            else {
                logger.error("failed get alarm of redis for restore.", get.cause());
                redisFut.fail(get.cause());
            }
        });

        Future<Boolean> mysqlFut = Future.future();
        mysqlAlarmRepository.get(where).setHandler(get -> {
            if(get.succeeded()){
                if(get.result() != null) {
                    DeviceAlarmEntity targetEntity = get.result();
                    targetEntity.setAlarmStatus((short) 0);
                    targetEntity.setAlarmRestoreTime(new Date());

                    mysqlAlarmRepository.update(targetEntity).setHandler(u -> {
                        if (u.succeeded()) {
                            logger.trace("update alarm of db for restore successfully.");
                            mysqlFut.complete(u.result());
                        } else {
                            logger.error("failed update alarm of db for restore.", u.cause());
                            mysqlFut.fail(u.cause());
                        }
                    });
                }
                else {
                    mysqlFut.complete(false);
                }
            }
            else {
                logger.error("failed get alarm of db for restore.");
                mysqlFut.fail(get.cause());
            }
        });

        CompositeFuture.all(redisFut, mysqlFut).setHandler(res -> {
            if(res.succeeded()) {
                logger.trace("update alarm restore time successfully.");
                result.complete(true);
            }
            else {
                logger.error("Failed update alarm restore time.", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }


}
