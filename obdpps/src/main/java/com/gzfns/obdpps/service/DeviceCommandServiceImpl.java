package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.DeviceCommandEntity;
import com.gzfns.obdpps.repository.mysql.MysqlDeviceCommandRepositoryImpl;
import com.gzfns.obdpps.repository.redis.RedisDeviceCommandRepositoryImpl;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class DeviceCommandServiceImpl implements IDeviceCommandService<DeviceCommandEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DeviceCommandServiceImpl.class);

    private Vertx vertx;

    public DeviceCommandServiceImpl(Vertx vertx){
        this.vertx = vertx;
    }

    @Override
    public Future<List<DeviceCommandEntity>> getRedisList(DeviceCommandEntity entity) {
        Future<List<DeviceCommandEntity>> result = Future.future();

        RedisDeviceCommandRepositoryImpl redisDeviceCommandRepository = new RedisDeviceCommandRepositoryImpl(vertx);
        redisDeviceCommandRepository.getList(entity).setHandler(res -> {
           if(res.succeeded()){
               logger.trace("Get command list from redis successfully.");
               result.complete(res.result());
           }
           else {
               logger.error("Failed get command list from redis.", res.cause());
               result.fail(res.cause());
           }
        });

        return result;
    }

    @Override
    public Future<Boolean> saveList(List<DeviceCommandEntity> entityList) {
        Future<Boolean> result = Future.future();

        for (DeviceCommandEntity entity : entityList) {
            RedisDeviceCommandRepositoryImpl redisDeviceCommandRepository = new RedisDeviceCommandRepositoryImpl(vertx);
            MysqlDeviceCommandRepositoryImpl mysqlDeviceCommandRepository = new MysqlDeviceCommandRepositoryImpl(vertx);
            Future<Boolean> redisFut = redisDeviceCommandRepository.insert(entity);
            Future<Boolean> mysqlFut = mysqlDeviceCommandRepository.insert(entity);
            CompositeFuture.all(redisFut,mysqlFut).setHandler(res -> {
                if(res.succeeded()){
                    logger.trace("saveList: save device command successfully.");
                }
                else {
                    logger.error("saveList: save device command failed.", res.cause());
                    result.fail(res.cause());
                }
            });
        }

        if(!result.failed()){
            result.complete(true);
        }

        return result;
    }

    @Override
    public Future<Boolean> updateReplyStatus(DeviceCommandEntity deviceCommandEntity) {
        Future<Boolean> result = Future.future();

        MysqlDeviceCommandRepositoryImpl mysqlDeviceCommandRepository = new MysqlDeviceCommandRepositoryImpl(vertx);
        mysqlDeviceCommandRepository.get(deviceCommandEntity).setHandler(res -> {
            if(res.succeeded()){
                logger.trace("updateReplyStatus: Get device command successfully.");
                DeviceCommandEntity old = res.result();
                old.setIsReply(true);
                old.setReplyTime(new Date());

                mysqlDeviceCommandRepository.update(old).setHandler(r -> {
                    if(r.succeeded()){
                        logger.trace("updateReplyStatus: update device command reply status successfully.");
                        result.complete(r.result());
                    }
                    else {
                        logger.error("updateReplyStatus: Failed update device command reply status.", r.cause());
                        result.fail(r.cause());
                    }
                });
            }
            else {
                logger.error("updateReplyStatus: Failed get device command.", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }

    @Override
    public Future<Boolean> updateSendStatus(DeviceCommandEntity entity) {
        Future<Boolean> result = Future.future();

        RedisDeviceCommandRepositoryImpl redisDeviceCommandRepository = new RedisDeviceCommandRepositoryImpl(vertx);
        MysqlDeviceCommandRepositoryImpl mysqlDeviceCommandRepository = new MysqlDeviceCommandRepositoryImpl(vertx);

        //指令已成功下发，从redis删除
        Future<Boolean> redisFut = Future.future();
        redisDeviceCommandRepository.delete(entity).setHandler(res -> {
            if(res.succeeded()){
                logger.trace("delete command from redis successfully. imei: {}", entity.getImei());
                redisFut.complete(res.result());
            }
            else {
                logger.error("Failed delete command from redis.", res.cause());
                redisFut.fail(res.cause());
            }
        });

        Future<Boolean> mysqlFut = Future.future();
        mysqlDeviceCommandRepository.get(entity).setHandler(res -> {
            if(res.succeeded()){
                logger.trace("Get command from mysql successfully.");
                DeviceCommandEntity old = res.result();
                old.setIsSend(true);
                old.setSendTime(new Date());

                mysqlDeviceCommandRepository.update(old).setHandler(r -> {
                    if(r.succeeded()){
                        logger.trace("update command send status to mysql successfully. imei: {}", old.getImei());
                        mysqlFut.complete(r.result());
                    }
                    else {
                        logger.error("Failed update command send status to mysql.", r.cause());
                        mysqlFut.fail(r.cause());
                    }
                });
            }
            else {
                logger.error("Failed get command from mysql.", res.cause());
                mysqlFut.fail(res.cause());
            }
        });

        CompositeFuture.all(redisFut,mysqlFut).setHandler(v -> {
            if(v.succeeded()){
                logger.trace("Update command send status to redis and mysql successfully.");
                result.complete(true);
            }
            else {
                logger.error("Failed update command send status to redis and mysql.", v.cause());
                result.fail(v.cause());
            }
        });

        return result;
    }


}
