package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.entity.DeviceVehicleEntity;
import com.gzfns.obdpps.entity.HeartBeatEntity;
import com.gzfns.obdpps.service.DeviceVehicleServiceImpl;
import com.gzfns.obdpps.service.HeartbeatServiceImpl;
import com.gzfns.obdpps.service.IDeviceVehicleService;
import com.gzfns.obdpps.service.IHeartbeatService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommonServiceVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(CommonServiceVerticle.class);

    private static int heartbeatBatchSize;
    private static Long immedSaveTimerInterval;
    private Long waitToCancelTimerId = 0L;

    private static final String ADDR_HEARTBEAT = "address_heartbeat";
    private static final String ADDR_VIN = "address_vin";

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();

        heartbeatBatchSize = config().getJsonObject("common-service").getInteger("heartbeat_batch_size", 10);
        immedSaveTimerInterval = Vertx.currentContext().config()
                .getJsonObject("common-service")
                .getLong("immed-save-timer-interval", 2000L);

        heartbeatReceivedNotified();

        vinReceivedNotified();

        startFuture.complete();
    }

    private void heartbeatReceivedNotified(){
        vertx.eventBus().<Buffer>consumer(ADDR_HEARTBEAT, msg -> {
            //取消立即保存
            logger.trace("heartbeatReceivedNotified, waitToCancelTimerId: " + waitToCancelTimerId);
            vertx.cancelTimer(waitToCancelTimerId);

            Buffer bufHeartbeat = msg.body();
            logger.trace("get heart beat from event bus: address_heartbeat, heartbeat: " + bufHeartbeat.toString());
            HeartBeatEntity heartBeatEntity = Json.decodeValue(bufHeartbeat, HeartBeatEntity.class);

            IHeartbeatService heartbeatService = new HeartbeatServiceImpl(vertx);
            heartbeatService.insert(heartBeatEntity).setHandler(insRes -> {
                if(insRes.succeeded()){
                    logger.trace("insert into redis successfully.");
                } else {
                    logger.error("Failed insert into redis.");
                }
            });

            //根据heartbeatBatchSize判断是否写入数据库
            heartbeatService.batchSave(vertx, heartBeatEntity, heartbeatBatchSize, false).setHandler(batchRes -> {
                if (batchRes.succeeded()) {
                    immedBatchSaveHeartbeatTimer();
                    logger.trace("heartbeatService.batchSave successfully. startSaving: false");
                } else {
                    logger.error("Failed heartbeatService.batchSave. startSaving: false");
                }
            });

        });
    }

    private void immedBatchSaveHeartbeatTimer(){
        waitToCancelTimerId = vertx.setTimer(immedSaveTimerInterval, id -> {
            IHeartbeatService heartbeatService = new HeartbeatServiceImpl(vertx);
            //立即存入数据库
            heartbeatService.batchSave(vertx, null, heartbeatBatchSize, true).setHandler(immedRes -> {
                if (immedRes.succeeded()) {
                    logger.trace("heartbeatService.batchSave successfully. startSaving: true");
                } else {
                    logger.error("Failed heartbeatService.batchSave. startSaving: true");
                }
            });
        });
        logger.trace("immedBatchSaveHeartbeatTimer, waitToCancelTimerId: " + waitToCancelTimerId);
    }

    private void vinReceivedNotified(){
        vertx.eventBus().<Buffer>consumer(ADDR_VIN, msg -> {
            Buffer bufVin = msg.body();
            logger.trace("get vin from event bus: address_vin, vinEntity: " + bufVin.toString());
            DeviceVehicleEntity deviceVehicleEntity = Json.decodeValue(bufVin, DeviceVehicleEntity.class);

            IDeviceVehicleService deviceVehicleService = new DeviceVehicleServiceImpl(vertx);
            deviceVehicleService.insert(deviceVehicleEntity).setHandler(insRes -> {
                if(insRes.succeeded()){
                    logger.trace("deviceVehicleService.insert successfully.");
                } else {
                    logger.error("Failed deviceVehicleService.insert.");
                }
            });
        });
    }
}
