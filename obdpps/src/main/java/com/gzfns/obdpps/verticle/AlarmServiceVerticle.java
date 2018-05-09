package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.entity.DeviceAlarmEntity;
import com.gzfns.obdpps.entity.HeartBeatEntity;
import com.gzfns.obdpps.service.AlarmServiceImpl;
import com.gzfns.obdpps.service.HeartbeatServiceImpl;
import com.gzfns.obdpps.service.IAlarmService;
import com.gzfns.obdpps.service.IHeartbeatService;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AlarmServiceVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(AlarmServiceVerticle.class);

    private static final Map<String, JsonObject> mapAlarmCondition = new ConcurrentHashMap<>();

    private static Long heartbeatInterval;
    private static int externalVoltageAlarmAfterTimes;
    private static Long externalVoltageThresholdValue;
    private static int powerOffAlarmAfterTimes;

    private static final String DDBJAlarmCode = "DDBJ";
    private static final String WDYBJAlarmCode = "WDYBJ";
    private static final String LXBJAlarmCode = "LXBJ";

    private static final String ADDR_HeartbeatRestored = "address_heartbeatRestored";
    private static final String ADDR_ExternalVoltage = "address_externalVoltage";
    private static final String ADDR_StatusAndAlarmCode = "address_statusAndAlarmCode";

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();

        Long offlineAlarmIntervalStarting = Vertx.currentContext().config()
                .getJsonObject("alarm-service")
                .getLong("offline-alarm-interval-starting", 300000L);

        Long offlineAlarmInterval = Vertx.currentContext().config()
                .getJsonObject("alarm-service")
                .getLong("offline-alarm-interval", 180000L);

        heartbeatInterval = Vertx.currentContext().config()
                .getLong("heart-beat-interval", 180000L);

        externalVoltageAlarmAfterTimes = Vertx.currentContext().config()
                .getJsonObject("alarm-service")
                .getInteger("external-voltage-alarm-after-times", 2);

        externalVoltageThresholdValue = Vertx.currentContext().config()
                .getJsonObject("alarm-service")
                .getLong("external-voltage-threshold-value", 10000L);

        powerOffAlarmAfterTimes = Vertx.currentContext().config()
                .getJsonObject("alarm-service")
                .getInteger("power-off-alarm-after-times", 2);

        vertx.setTimer(offlineAlarmIntervalStarting, offlineAlarm(offlineAlarmInterval));

        heartbeatRestoredNotified();
        externalVoltageAlarm();
        poweroffAlarm();

        startFuture.complete();
    }

    //离线报警
    private Handler<Long> offlineAlarm(Long offlineAlarmInterval){
        return l -> {
            IHeartbeatService heartbeatService = new HeartbeatServiceImpl(vertx);
            heartbeatService.getHeartbeatListBy(0).setHandler(getListRes -> {
                if(getListRes.succeeded()){
                    List<Future> futures = new ArrayList<>();
                    List<HeartBeatEntity> heartBeatEntityList = getListRes.result();
                    for (HeartBeatEntity heartbeat : heartBeatEntityList) {
                        if(!hasHeartBeat(heartbeat.getHeartbeatTime(), heartbeatInterval)){//没有心跳包
                            logger.trace("Hasn't heart beat, insert LXBJ alarm. imei: {}", heartbeat.getImei());
                            DeviceAlarmEntity offlineAlarm = alarmEntityBuilder(heartbeat.getImei(), LXBJAlarmCode, "");

                            IAlarmService alarmService = new AlarmServiceImpl(vertx);
                            alarmService.insert(offlineAlarm).setHandler(insertRes -> {
                                if(insertRes.succeeded()){
                                    heartbeat.setHadOfflineAlarm(1);
                                    Future<Boolean> updateHadOfflineAlarmFut = heartbeatService.updateHadOfflineAlarm(heartbeat);
                                    futures.add(updateHadOfflineAlarmFut);
                                }
                                else {
                                    futures.add(Future.failedFuture(insertRes.cause()));
                                }
                            });
                        }
                    }

                    CompositeFuture.all(futures).setHandler(insertAlarmRes -> {
                       if(insertAlarmRes.succeeded()){
                           logger.trace("insert all offline alarm and update HadOfflineAlarm to redis and mysql successfully.");
                       }
                       else {
                           logger.error("Failed insert all offline alarm and update HadOfflineAlarm to redis or mysql.", insertAlarmRes.cause());
                       }
                    });
                }
                else {
                    logger.trace("Failed get heart beat list.", getListRes.cause());
                }
            });

            vertx.setTimer(offlineAlarmInterval, offlineAlarm(offlineAlarmInterval));
        };
    }

    private Boolean hasHeartBeat(Date heartBeatTime, Long heartbeatInterval){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
        logger.trace("heartBeatTime: " + sdf.format(heartBeatTime) + ", Curr: " + sdf.format(new Date()));
        Calendar dateOne=Calendar.getInstance(),dateTwo=Calendar.getInstance();
        dateOne.setTime(new Date());
        dateTwo.setTime(heartBeatTime);

        long timeOne=dateOne.getTimeInMillis();
        long timeTwo=dateTwo.getTimeInMillis();

        long intervalMillis=Math.abs(timeOne-timeTwo);

        return heartbeatInterval - intervalMillis > 0;
    }

    //心跳恢复
    private void heartbeatRestoredNotified(){
        vertx.eventBus().<String>consumer(ADDR_HeartbeatRestored, message -> {
            String imei = message.body();
            updateAlarmRestore(imei, LXBJAlarmCode);
        });
    }

    //外电压报警
    private void externalVoltageAlarm() {
        vertx.eventBus().<JsonObject>consumer(ADDR_ExternalVoltage, message -> {
            JsonObject jsonMessage = message.body();
            final String imei = jsonMessage.getString("imei");
            final String strExternalVoltage = jsonMessage.getString("externalVoltage");

            long externalVoltage = Long.parseLong(strExternalVoltage);

            //是否已经报警
            Boolean ifAlreadyAlarm = getIfAlreadyAlarm(imei, WDYBJAlarmCode);

            logger.trace("imei: " + imei
                    + ", external voltage ifAlreadyAlarm: " + ifAlreadyAlarm
                    + ", externalVoltage: " + externalVoltage);

            if(externalVoltage > 0) {//externalVoltage=0时什么也不干
                if (externalVoltage < externalVoltageThresholdValue) {
                    if (!ifAlreadyAlarm) {
                        //判断连续上传报警次数
                        int alarmReceivedTimes = getReceivedTimes(imei, WDYBJAlarmCode, "AlarmReceivedTimes") + 1;
                        logger.trace("external voltage alarm received times: " + alarmReceivedTimes);
                        if(alarmReceivedTimes >= externalVoltageAlarmAfterTimes) {
                            insertAlarm(imei, WDYBJAlarmCode, "");

                            logger.trace("update external voltage alarm ifAlreadyAlarm: {}, AlarmReceivedTimes: {}", true, 0);
                            updateIfAlreadyAlarm(imei, WDYBJAlarmCode, true);
                            updateReceivedTimes(imei, WDYBJAlarmCode, "AlarmReceivedTimes", 0);
                        }
                        else {
                            logger.trace("update external voltage alarm AlarmReceivedTimes: {}", alarmReceivedTimes + 1);
                            updateReceivedTimes(imei, WDYBJAlarmCode, "AlarmReceivedTimes", alarmReceivedTimes + 1);
                        }
                    }
                    else {
                        logger.trace("ifAlreadyAlarm = true, update external voltage RestoreReceivedTimes: {}", 0);
                        updateReceivedTimes(imei, WDYBJAlarmCode, "RestoreReceivedTimes", 0);
                    }
                } else {
                    if (ifAlreadyAlarm) {
                        //判断连续上传恢复次数
                        int restoreReceivedTimes = getReceivedTimes(imei, WDYBJAlarmCode, "RestoreReceivedTimes") + 1;
                        logger.trace("external voltage restore received times: " + restoreReceivedTimes);
                        if(restoreReceivedTimes >= externalVoltageAlarmAfterTimes) {
                            updateAlarmRestore(imei, WDYBJAlarmCode);

                            logger.trace("update external voltage alarm ifAlreadyAlarm: {}, RestoreReceivedTimes: {}", true, 0);
                            updateIfAlreadyAlarm(imei, WDYBJAlarmCode, false);
                            updateReceivedTimes(imei, WDYBJAlarmCode, "RestoreReceivedTimes", 0);
                        }
                        else {
                            logger.trace("update external voltage alarm RestoreReceivedTimes: {}", restoreReceivedTimes + 1);
                            updateReceivedTimes(imei, WDYBJAlarmCode, "RestoreReceivedTimes", restoreReceivedTimes + 1);
                        }
                    }
                    else {
                        logger.trace("ifAlreadyAlarm = false, update external voltage AlarmReceivedTimes: {}", 0);
                        updateReceivedTimes(imei, WDYBJAlarmCode, "AlarmReceivedTimes", 0);
                    }
                }
            }
        });
    }

    //断电报警
    private void poweroffAlarm(){
        vertx.eventBus().<JsonObject>consumer(ADDR_StatusAndAlarmCode, message -> {
            JsonObject jsonMessage = message.body();
            final String imei = jsonMessage.getString("imei");
            final String strStatusAndAlarmCode = jsonMessage.getString("statusAndAlarmCode");

            //0100000020
            logger.trace("strStatusAndAlarmCode: " + strStatusAndAlarmCode);
            String strAlarmCode = strStatusAndAlarmCode.substring(5);
            String A3 = strAlarmCode.substring(3, 4);

            Boolean ifAlreadyAlarm = getIfAlreadyAlarm(imei, DDBJAlarmCode);

            switch(A3)
            {
                case "2":
                    logger.trace("A3=2, ifAlreadyAlarm: " + ifAlreadyAlarm);
                    if(!ifAlreadyAlarm)
                    {
                        //判断连续上传报警次数
                        int alarmReceivedTimes = getReceivedTimes(imei, DDBJAlarmCode, "AlarmReceivedTimes") + 1;
                        logger.trace("DDBJ alarm received times: " + alarmReceivedTimes);
                        if(alarmReceivedTimes >= powerOffAlarmAfterTimes) {
                            insertAlarm(imei, DDBJAlarmCode, "");
                            logger.trace("insert DDBJ alarm successfully.");

                            logger.trace("update DDBJ alarm ifAlreadyAlarm: {}, AlarmReceivedTimes: {}", true, 0);
                            updateIfAlreadyAlarm(imei, DDBJAlarmCode, true);
                            updateReceivedTimes(imei, DDBJAlarmCode, "AlarmReceivedTimes", 0);
                        }
                        else {
                            logger.trace("update DDBJ alarm AlarmReceivedTimes: {}", alarmReceivedTimes + 1);
                            updateReceivedTimes(imei, DDBJAlarmCode, "AlarmReceivedTimes", alarmReceivedTimes + 1);
                        }
                    }
                    else {
                        logger.trace("ifAlreadyAlarm = true, update DDBJ RestoreReceivedTimes: {}", 0);
                        updateReceivedTimes(imei, DDBJAlarmCode, "RestoreReceivedTimes", 0);
                    }
                    break;
                case "0"://说明没有报警或者断电报警以及低电压报警均已经恢复
                    logger.trace("A3=0, ifAlreadyAlarm: " + ifAlreadyAlarm);
                    if(ifAlreadyAlarm)
                    {
                        //判断连续上传恢复次数
                        int restoreReceivedTimes = getReceivedTimes(imei, DDBJAlarmCode, "RestoreReceivedTimes") + 1;
                        logger.trace("DDBJ restore received times: " + restoreReceivedTimes);
                        if(restoreReceivedTimes >= powerOffAlarmAfterTimes) {
                            updateAlarmRestore(imei, DDBJAlarmCode);
                            logger.trace("update DDBJ record for restore successfully.");

                            logger.trace("update DDBJ alarm ifAlreadyAlarm: {}, RestoreReceivedTimes: {}", true, 0);
                            updateIfAlreadyAlarm(imei, DDBJAlarmCode, false);
                            updateReceivedTimes(imei, DDBJAlarmCode, "RestoreReceivedTimes", 0);
                        }
                        else {
                            logger.trace("update DDBJ alarm RestoreReceivedTimes: {}", restoreReceivedTimes + 1);
                            updateReceivedTimes(imei, DDBJAlarmCode, "RestoreReceivedTimes", restoreReceivedTimes + 1);
                        }
                    }
                    else {
                        logger.trace("ifAlreadyAlarm = false, update DDBJ AlarmReceivedTimes: {}", 0);
                        updateReceivedTimes(imei, DDBJAlarmCode, "AlarmReceivedTimes", 0);
                    }
                    break;
                default:
                    break;
            }
        });
    }

    private Boolean getIfAlreadyAlarm(String imei, String alarmCode){
        Boolean ifAlreadyAlarm = false;
        JsonObject alarmCondition = getAlarmCondition(imei, alarmCode);
        if(alarmCondition != null){
            ifAlreadyAlarm = alarmCondition.getBoolean("IfAlreadyAlarm", false);
        }
        return ifAlreadyAlarm;
    }

    private void updateIfAlreadyAlarm(String imei, String alarmCode, Boolean ifAlreadyAlarm){
        JsonObject alarmCondition = getAlarmCondition(imei, alarmCode);
        if(ObjectUtils.equals(alarmCondition, null)){
            alarmCondition = new JsonObject().put("IfAlreadyAlarm", ifAlreadyAlarm);
        }
        else {
            alarmCondition.put("IfAlreadyAlarm", ifAlreadyAlarm);
        }

        updateAlarmCondition(imei, alarmCode, alarmCondition);
    }

    private int getReceivedTimes(String imei, String alarmCode, String rtKey){
        int receivedTimes = 0;
        JsonObject alarmCondition = getAlarmCondition(imei, alarmCode);
        if(alarmCondition != null){
            receivedTimes = alarmCondition.getInteger(rtKey, 0);
        }
        return receivedTimes;
    }

    private void updateReceivedTimes(String imei, String alarmCode, String rtKey, int newReceivedTimes){
        JsonObject alarmCondition = getAlarmCondition(imei, alarmCode);
        if(ObjectUtils.equals(alarmCondition, null)){
            alarmCondition = new JsonObject().put(rtKey, newReceivedTimes);
        }
        else {
            alarmCondition.put(rtKey, newReceivedTimes);
        }

        updateAlarmCondition(imei, alarmCode, alarmCondition);
    }

    private JsonObject getAlarmCondition(String imei, String alarmCode){
        JsonObject alarmCondition = null;
        if(mapAlarmCondition.containsKey(imei)){
            JsonObject alarmJson = mapAlarmCondition.get(imei);
            logger.trace("imei: " + imei + ", alarmJson: " + alarmJson.encodePrettily());
            if(alarmJson.containsKey(alarmCode)) {
                alarmCondition = alarmJson.getJsonObject(alarmCode);
            }
        }

        return alarmCondition;
    }

    private void updateAlarmCondition(String imei, String alarmCode, JsonObject alarmCondition){
        if(mapAlarmCondition.containsKey(imei)){
            JsonObject alarmJson = mapAlarmCondition.get(imei);
            alarmJson.put(alarmCode, alarmCondition);
        }
        else {
            JsonObject alarmJson = new JsonObject().put(alarmCode, alarmCondition);
            mapAlarmCondition.put(imei, alarmJson);
        }
    }

    private DeviceAlarmEntity alarmEntityBuilder(String imei, String alarmCode, String alarmThresholdValue){
        DeviceAlarmEntity deviceAlarm = new DeviceAlarmEntity();
        deviceAlarm.setImei(imei);
        deviceAlarm.setAlarmCode(alarmCode);
        deviceAlarm.setAlarmTime(new Date());
        deviceAlarm.setAlarmStatus((short)1);
        deviceAlarm.setAlarmThresholdValue(alarmThresholdValue);

        return deviceAlarm;
    }

    private void insertAlarm(String imei, String alarmCode, String alarmThresholdValue){
        DeviceAlarmEntity deviceAlarm = alarmEntityBuilder(imei, alarmCode, alarmThresholdValue);
        IAlarmService alarmService = new AlarmServiceImpl(vertx);
        alarmService.insert(deviceAlarm).setHandler(insertRes -> {
            if(insertRes.succeeded()){
                logger.trace("insert " + alarmCode + " alarm to redis and mysql successfully.");
            }
            else {
                logger.error("Failed insert " + alarmCode + " alarm to redis or mysql.", insertRes.cause());
            }
        });
    }

    private void updateAlarmRestore(String imei, String alarmCode){
        DeviceAlarmEntity where = new DeviceAlarmEntity();
        where.setImei(imei);
        where.setAlarmCode(alarmCode);
        where.setAlarmStatus((short)1);
        IAlarmService alarmService = new AlarmServiceImpl(vertx);
        alarmService.updateBy(where).setHandler(updateRes -> {
            if(updateRes.succeeded()){
                logger.trace("update " + alarmCode + " alarm status and restore time successfully.");
            }
            else {
                logger.error("Failed update " + alarmCode + " alarm status and restore time.", updateRes.cause());
            }
        });
    }
}
