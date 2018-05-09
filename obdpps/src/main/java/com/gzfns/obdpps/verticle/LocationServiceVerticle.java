package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.entity.ObdDataEntity;
import com.gzfns.obdpps.service.IObdDataService;
import com.gzfns.obdpps.service.ObdDataServiceImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocationServiceVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(LocationServiceVerticle.class);
    private static int locationBatchSize;
    private static Long immedSaveTimerInterval;
    private Long waitToCancelTimerId = 0L;

    private static final IObdDataService obdDataService = new ObdDataServiceImpl();

    private static final String ADDR_ObdData = "address_obdData";

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();
        locationBatchSize = config().getJsonObject("location-service").getInteger("location_batch_size", 5);
        immedSaveTimerInterval = Vertx.currentContext().config()
                .getJsonObject("location-service")
                .getLong("immed-save-timer-interval", 2000L);

        obdDataReceivedNotified();
        startFuture.complete();
    }

    private void obdDataReceivedNotified(){
        vertx.eventBus().<Buffer>consumer(ADDR_ObdData, msg -> {
            //取消立即保存
            logger.trace("obdDataReceivedNotified, waitToCancelTimerId: " + waitToCancelTimerId);
            vertx.cancelTimer(waitToCancelTimerId);

            Buffer bufObdData = msg.body();
            logger.trace("get obd data from event bus: address_obdData, ObdData: " + bufObdData.toString());
            ObdDataEntity obdDataEntity = Json.decodeValue(bufObdData, ObdDataEntity.class);

            //根据mysqlBatchSize判断是否写入数据库
            obdDataService.batchSave(vertx, obdDataEntity, locationBatchSize, false).setHandler(batchRes -> {
                if (batchRes.succeeded()) {
                    immedBatchSaveTimer();
//                    msg.reply(true);
                    logger.trace("obdDataService.batchSave successfully. startSaving: false");
                } else {
//                    msg.fail(500, "Failed to batch save.");
                    logger.error("Failed obdDataService.batchSave. startSaving: false");
                }
            });

        });
    }

    private void immedBatchSaveTimer(){
        waitToCancelTimerId = vertx.setTimer(immedSaveTimerInterval, id -> {
            //立即存入数据库
            obdDataService.batchSave(vertx, null, locationBatchSize, true).setHandler(immedRes -> {
                if (immedRes.succeeded()) {
                    logger.trace("obdDataService.batchSave successfully. startSaving: true");
                } else {
                    logger.error("Failed obdDataService.batchSave. startSaving: true");
                }
            });
        });
        logger.trace("immedBatchSaveTimer, waitToCancelTimerId: " + waitToCancelTimerId);
    }
}
