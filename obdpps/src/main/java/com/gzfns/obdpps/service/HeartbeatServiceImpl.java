package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.HeartBeatEntity;
import com.gzfns.obdpps.repository.mysql.MysqlHeartBeatRepositoryImpl;
import com.gzfns.obdpps.repository.redis.RedisHeartBeatRepositoryImpl;
import com.gzfns.obdpps.util.CglibBeanCopierUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatServiceImpl implements IHeartbeatService {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatServiceImpl.class);

    private static Map<String, HeartBeatEntity> waitToBatchSaveHeartbeatMap = new ConcurrentHashMap<>();

    private static int counter = 0;

    private RedisHeartBeatRepositoryImpl redisHeartBeatRepository;
    private MysqlHeartBeatRepositoryImpl mysqlHeartBeatRepository;

    public HeartbeatServiceImpl(Vertx vertx){
        this.redisHeartBeatRepository = new RedisHeartBeatRepositoryImpl(vertx);
        this.mysqlHeartBeatRepository = new MysqlHeartBeatRepositoryImpl(vertx);
    }


    @Override
    public Future<Boolean> batchSave(Vertx vertx, HeartBeatEntity heartBeatEntity, int batchSize, boolean startSaving) {
        Future<Boolean> result = Future.future();

        vertx.executeBlocking(deepCopyFuture -> {

            if(!ObjectUtils.equals(heartBeatEntity, null)) {
                //计数器
                counter = counter + 1;

//                if (!waitToBatchSaveHeartbeatMap.containsKey(String.valueOf(counter))) {
                    waitToBatchSaveHeartbeatMap.put(String.valueOf(counter), heartBeatEntity);
//                } else {
//                    listOfHistoryTable = waitToBatchSaveByHistoryTableNameMap.get(tableName);
//                }
            }

            logger.trace("Now heartBeatEntity counter: " + counter);

            //立即处理
            if(counter > 0 && startSaving){
                //深度复制map
                Map<String, HeartBeatEntity> deepcopiedMap = new ConcurrentHashMap<>();
                try {
                    for (String key : waitToBatchSaveHeartbeatMap.keySet()) {
                        HeartBeatEntity srcHeartbeatEntity = waitToBatchSaveHeartbeatMap.get(key);
                        HeartBeatEntity deepCopiedEntity = new HeartBeatEntity();
                        CglibBeanCopierUtil.copyProperties(srcHeartbeatEntity, deepCopiedEntity);

                        deepcopiedMap.put(key, deepCopiedEntity);
                    }

                    //将waitToBatchSaveHeartbeatMap置于初始化状态
                    waitToBatchSaveHeartbeatMap = new ConcurrentHashMap<>();
                    counter = 0;

                    deepCopyFuture.complete(deepcopiedMap);
                }
                catch (Exception e){
                    logger.error("Deep copy error.startSaving=true", e.getCause());
                    deepCopyFuture.fail(e.getCause());
                }
            }
            else if(counter > 0 && counter >= batchSize){//检查是否达到批量处理的阈值
                //深度复制map
                Map<String, HeartBeatEntity> deepcopiedMap = new ConcurrentHashMap<>();
                try {
                    for (String key : waitToBatchSaveHeartbeatMap.keySet()) {
                        HeartBeatEntity srcHeartbeatEntity = waitToBatchSaveHeartbeatMap.get(key);
                        HeartBeatEntity deepCopiedEntity = new HeartBeatEntity();
                        CglibBeanCopierUtil.copyProperties(srcHeartbeatEntity, deepCopiedEntity);

                        deepcopiedMap.put(key, deepCopiedEntity);
                    }

                    //将waitToBatchSaveHeartbeatMap置于初始化状态
                    waitToBatchSaveHeartbeatMap = new ConcurrentHashMap<>();
                    counter = 0;

                    deepCopyFuture.complete(deepcopiedMap);
                }
                catch (Exception e){
                    logger.error("Deep copy error.startSaving=false", e.getCause());
                    deepCopyFuture.fail(e.getCause());
                }
            }
            else {
                deepCopyFuture.complete(null);
            }
        }, res -> {
            if(res.succeeded()){
                Object execResult = res.result();
                if(!ObjectUtils.equals(execResult, null))
                {
                    Map<String, HeartBeatEntity> resultMap = (Map<String, HeartBeatEntity>)res.result();

                    List<HeartBeatEntity> savingHeartbeatList = new ArrayList<>();
                    for (String key : resultMap.keySet()) {
                        savingHeartbeatList.add(resultMap.get(key));
                    }

                    MysqlHeartBeatRepositoryImpl mysqlRepository = new MysqlHeartBeatRepositoryImpl(vertx);

//                    Future<Boolean> mysqlFut = Future.future();
                    //存当前表
                    mysqlRepository.batchInsertOrUpdateHeartbeat(savingHeartbeatList, batchSize).setHandler(batchCurrRes -> {
                        if(batchCurrRes.succeeded()){
                            logger.trace("Batch insert or update heartbeat table successfully.");
                            result.complete(true);
                        }
                        else {
                            logger.error("Failed batch insert or update heartbeat table.", batchCurrRes.cause());
                            result.fail(batchCurrRes.cause());
                        }
                    });
                }
                else {
                    logger.trace("Has't execute insert to DB, because the counter is equal to 0 or less than heartbeat batchSize.");
                    result.complete(true);
                }
            }
            else {
                logger.error("executeBlocking Failed", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }

    @Override
    public Future<Boolean> insert(HeartBeatEntity heartBeatEntity) {
        Future<Boolean> result = Future.future();

        redisHeartBeatRepository.insert(heartBeatEntity).setHandler(res -> {
            if(res.succeeded()){
                logger.trace("insert heart beat entity to redis successfully.");
                result.complete(true);
            }
            else {
                logger.error("Failed insert heart beat entity to redis.", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }

    @Override
    public Future<List<HeartBeatEntity>> getHeartbeatListBy(int hadOfflineAlarm) {
        Future<List<HeartBeatEntity>> result = Future.future();

        HeartBeatEntity where = new HeartBeatEntity();
        where.setHadOfflineAlarm(hadOfflineAlarm);

        mysqlHeartBeatRepository.getHeartbeatListByHadOfflineAlarm(where).setHandler(res -> {
           if(res.succeeded()){
               logger.trace("Get Heart beat list by hadOfflineAlarm successfully.");
               List<HeartBeatEntity> heartBeatEntityList = res.result();
               result.complete(heartBeatEntityList);
           }
           else {
               logger.error("Failed get heart beat list by hadOfflineAlarm.", res.cause());
               result.fail(res.cause());
           }
        });


        return result;
    }

    @Override
    public Future<Boolean> updateHadOfflineAlarm(HeartBeatEntity heartBeatEntity) {
        Future<Boolean> result = Future.future();

        Future<Boolean> redisFut = redisHeartBeatRepository.insert(heartBeatEntity);
        Future<Boolean> mysqlFut = mysqlHeartBeatRepository.updateByImei(heartBeatEntity);

        CompositeFuture.all(redisFut, mysqlFut).setHandler(updateRes -> {
           if(updateRes.succeeded()){
               logger.trace("Update heart beat HadOfflineAlarm successfully.");
               result.complete(true);
           }
           else {
               logger.error("Failed update heart beat HadOfflineAlarm.", updateRes.cause());
               result.fail(updateRes.cause());
           }
        });

        return result;
    }
}
