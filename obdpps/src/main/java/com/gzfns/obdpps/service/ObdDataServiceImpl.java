package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.ObdDataEntity;
import com.gzfns.obdpps.repository.mysql.MysqlObdDataRepositoryImpl;
import com.gzfns.obdpps.util.CglibBeanCopierUtil;
import com.gzfns.obdpps.util.HashUtil;
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

public class ObdDataServiceImpl implements IObdDataService {

    private static final Logger logger = LoggerFactory.getLogger(ObdDataServiceImpl.class);

    private static Map<String, List<ObdDataEntity>> waitToBatchSaveByHistoryTableNameMap = new ConcurrentHashMap<>();

    private static int counter = 0;

    @Override
    public Future<Boolean> batchSave(Vertx vertx, ObdDataEntity obdDataEntity, int batchSize, boolean startSaving) {
        Future<Boolean> result = Future.future();

        vertx.executeBlocking(deepCopyFuture -> {

            if(!ObjectUtils.equals(obdDataEntity, null)) {
                //计数器
                counter = counter + 1;

                String tableName = "device_obd_data_" + HashUtil.getHashValue(obdDataEntity.getImei()) % 100;
                logger.trace("tableName: " + tableName);

                List<ObdDataEntity> listOfHistoryTable = new ArrayList<>();
                if (!waitToBatchSaveByHistoryTableNameMap.containsKey(tableName)) {
                    listOfHistoryTable.add(obdDataEntity);
                    waitToBatchSaveByHistoryTableNameMap.put(tableName, listOfHistoryTable);
                } else {
                    listOfHistoryTable = waitToBatchSaveByHistoryTableNameMap.get(tableName);
                    listOfHistoryTable.add(obdDataEntity);
                }
            }

            logger.trace("Now obdDataEntity counter: " + counter);

            //立即处理
            if(counter > 0 && startSaving){
                //深度复制map中的List
                Map<String, List<ObdDataEntity>> deepcopiedMap = new ConcurrentHashMap<>();
                try {
                    for (String key : waitToBatchSaveByHistoryTableNameMap.keySet()) {
                        List<ObdDataEntity> srcObdDataListOfKey = waitToBatchSaveByHistoryTableNameMap.get(key);
                        List<ObdDataEntity> immedObdDataListOfKey = new ArrayList<>();
                        for (ObdDataEntity dataEntity : srcObdDataListOfKey) {
                            ObdDataEntity deepCopiedEntity = new ObdDataEntity();
                            CglibBeanCopierUtil.copyProperties(dataEntity, deepCopiedEntity);
                            immedObdDataListOfKey.add(deepCopiedEntity);
                        }

                        if (immedObdDataListOfKey.size() > 0) {
                            deepcopiedMap.put(key, immedObdDataListOfKey);
                        } else {
                            logger.error("immediately saving. deep copy failed.");
                            throw new Exception("immediately saving. deep copy failed.");
                        }
                    }

                    //将waitToBatchSaveByHistoryTableNameMap置于初始化状态
                    waitToBatchSaveByHistoryTableNameMap = new ConcurrentHashMap<>();
                    counter = 0;

                    deepCopyFuture.complete(deepcopiedMap);
                }
                catch (Exception e){
                    logger.error("Deep copy error.", e.getCause());
                    deepCopyFuture.fail(e.getCause());
                }
            }
            else if(counter > 0 && counter >= batchSize){//检查是否达到批量处理的阈值
                //深度复制map中的List
                Map<String, List<ObdDataEntity>> deepcopiedMap = new ConcurrentHashMap<>();
                try {
                    for (String key : waitToBatchSaveByHistoryTableNameMap.keySet()) {
                        List<ObdDataEntity> srcObdDataListOfKey = waitToBatchSaveByHistoryTableNameMap.get(key);
                        List<ObdDataEntity> batchObdDataListOfKey = new ArrayList<>();
                        for (ObdDataEntity dataEntity : srcObdDataListOfKey) {
                            ObdDataEntity deepCopiedEntity = new ObdDataEntity();
                            CglibBeanCopierUtil.copyProperties(dataEntity, deepCopiedEntity);
                            batchObdDataListOfKey.add(deepCopiedEntity);
                        }

                        if (batchObdDataListOfKey.size() > 0) {
                            deepcopiedMap.put(key, batchObdDataListOfKey);
                        } else {
                            logger.error("batch saving. deep copy failed.");
                            throw new Exception("batch saving. deep copy failed.");
                        }
                    }

                    //将waitToBatchSaveByHistoryTableNameMap置于初始化状态
                    waitToBatchSaveByHistoryTableNameMap = new ConcurrentHashMap<>();
                    counter = 0;

                    deepCopyFuture.complete(deepcopiedMap);
                }
                catch (Exception e){
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
                    Map<String, List<ObdDataEntity>> resultMap = (Map<String, List<ObdDataEntity>>)res.result();

                    //所有记录合并至一个List
                    List<ObdDataEntity> savingObdDataList = new ArrayList<>();
                    for (String key : resultMap.keySet()) {
                        savingObdDataList.addAll(resultMap.get(key));
                    }

                    MysqlObdDataRepositoryImpl mysqlRepository = new MysqlObdDataRepositoryImpl(vertx);

                    Future<Boolean> currFut = Future.future();
                    //存当前表
                    mysqlRepository.batchInsertOrUpdateCurrent(savingObdDataList, batchSize).setHandler(batchCurrRes -> {
                       if(batchCurrRes.succeeded()){
                           logger.trace("Batch insert or update current table successfully.");
                           currFut.complete(true);
                       }
                       else {
                           logger.error("Failed batch insert or update current table.", batchCurrRes.cause());
                           currFut.fail(batchCurrRes.cause());
                       }
                    });

                    Future<Boolean> historyFut = Future.future();
                    //存历史表
                    mysqlRepository.batchInsertHistory(resultMap, batchSize).setHandler(batchHistoryRes -> {
                        if(batchHistoryRes.succeeded()){
                            logger.trace("Batch insert history table successfully.");
                            historyFut.complete(true);
                        }
                        else {
                            logger.error("Failed batch insert history table.", batchHistoryRes.cause());
                            historyFut.fail(batchHistoryRes.cause());
                        }
                    });

                    CompositeFuture.all(currFut, historyFut).setHandler(batchRes -> {
                       if(batchRes.succeeded()){
                           result.complete(true);
                       }
                       else {
                           result.fail(batchRes.cause());
                       }
                    });
                }
                else {
                    logger.trace("Has't execute insert to DB, because the counter is equal to 0 or less than batchSize.");
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
}
