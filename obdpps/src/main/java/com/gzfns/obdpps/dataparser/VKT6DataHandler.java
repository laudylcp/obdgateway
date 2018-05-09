package com.gzfns.obdpps.dataparser;

import com.gzfns.obdpps.command.vkt6.DataPackageReplyMessage;
import com.gzfns.obdpps.entity.DeviceCommandEntity;
import com.gzfns.obdpps.entity.DeviceVehicleEntity;
import com.gzfns.obdpps.entity.HeartBeatEntity;
import com.gzfns.obdpps.entity.ObdDataEntity;
import com.gzfns.obdpps.repository.redis.RedisObdDataRepositoryImpl;
import com.gzfns.obdpps.senddown.VKT6MessageReplyer;
import com.gzfns.obdpps.service.DeviceCommandServiceImpl;
import com.gzfns.obdpps.service.IDeviceCommandService;
import com.gzfns.obdpps.util.FileUtil;
import com.gzfns.obdpps.util.Gps;
import com.gzfns.obdpps.util.PositionUtils;
import com.gzfns.obdpps.util.StringUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

public class VKT6DataHandler implements Handler<Buffer> {

    private static final Logger logger = LoggerFactory.getLogger(VKT6DataHandler.class);
    private static Vertx vertx;
    private String writeHandlerID;
    private static Boolean isWriteRawData;
    private static String rawDataDir;

    private static final String MESSAGE_HEADER = "*MG20";
    private static final String MESSAGE_TAIL = "#";
    private static final String MESSAGE_SEPARATOR = "&";

//    private static final String OBDDATA_QUEUE_NAME = "queue-obd-data";
    private static final String ADDR_HeartbeatRestored = "address_heartbeatRestored";
    private static final String ADDR_ExternalVoltage = "address_externalVoltage";
    private static final String ADDR_StatusAndAlarmCode = "address_statusAndAlarmCode";
    private static final String ADDR_SocketReceived = "address_socketReceived";
    private static final String ADDR_ObdData = "address_obdData";
    private static final String ADDR_HEARTBEAT = "address_heartbeat";
    private static final String ADDR_VIN = "address_vin";

    public VKT6DataHandler(Vertx vertxInstance, String writeHandlerID){
        vertx = vertxInstance;
        this.writeHandlerID = writeHandlerID;
        JsonObject config = vertxInstance.getOrCreateContext().config();
        isWriteRawData = config.getJsonObject("raw-data").getBoolean("is-write", true);
        rawDataDir = config.getJsonObject("raw-data").getString("dir","RawData");
    }

    @Override
    public void handle(Buffer buffer) {
        logger.trace("-----------------Begin parse data----------------");
        String rowData = buffer.toString(Charset.forName("ASCII"));

//        String hexRawData = StringUtil.string2HexString(rowData);
//        logger.trace("Hex raw data: " + hexRawData);

        // Convert &Q and &U to hex string
        logger.trace("Before convert: {}", rowData);
        rowData = convertToHexFor(rowData, "ASCII", MESSAGE_SEPARATOR + "Q", 14);
        rowData = convertToHexFor(rowData, "ASCII", MESSAGE_SEPARATOR + "U", 16);
        logger.trace("After  convert: {}", rowData);

        if(rowData.length() > 5) {
            String header = rowData.substring(0, 5);
            if (StringUtils.equals(header, MESSAGE_HEADER)) {

                //Write raw data to file.
                if (isWriteRawData) {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    final String fileName = rawDataDir + "/" + dateFormat.format(new Date()) + ".txt";
                    FileUtil.writeToFile(rowData + MESSAGE_TAIL, vertx, rawDataDir, fileName).setHandler(res -> {
                        if (res.succeeded()) {
                            logger.trace("Write raw data to file successfully.");
                        } else {
                            logger.error("Failed write raw data to file.", res.cause());
                        }
                    });
                }

                //回复属性：1-需要回复，0-不需要回复
                String rspProperty = rowData.substring(5, 6);
                int indexOfComma = rowData.indexOf(',');
                //11位手机号或15位IMEI号
                String deviceNumber = rowData.substring(6, indexOfComma);

                //设备已经连接，可以下发指令
                sendDownNotify(vertx, deviceNumber, writeHandlerID);

                String functionStr;
                //附加信息
                String extDataInfo = "";
                int indexOfAnd = rowData.indexOf(MESSAGE_SEPARATOR);
                //*MG201868500027121029,YBI#
                if (indexOfAnd == -1) {
                    functionStr = rowData.substring(indexOfComma + 1);
                } else {
                    functionStr = rowData.substring(indexOfComma + 1, indexOfAnd);
                    //第一个&字符不取
                    extDataInfo = rowData.substring(indexOfAnd + 1);
                }
                logger.trace("functionStr: {}, extDataInfo: {}", functionStr, extDataInfo);

                //回复内容：Y
                String replyContent = "";
                //功能类型编号：A-上传状态类信息,B-上传定位类信息
                String functionTypeCode;
                //功能项关键字
                String functionItemKeyword;
                if (functionStr.length() > 2) {
                    replyContent = functionStr.substring(0, 1);
                    functionTypeCode = functionStr.substring(1, 2);
                    functionItemKeyword = functionStr.substring(2, 3);
                } else {
                    functionTypeCode = functionStr.substring(0, 1);
                    functionItemKeyword = functionStr.substring(1, 2);
                }

                //获得附加信息数组
                String[] arrExtDataInfos = extDataInfo.split(MESSAGE_SEPARATOR);
                String strGpsLocation = "";
                String strVin = "";
                String strStatusAndAlarmCode = "";
                String strTotalMileage = "";
//                String strTimestamp = "";
//                String strAltitude = "";
//                String strBatteryPercent = "";
//                String strGSM = "";
//                String strGpsSignalIntensity = "";
                String strExternalVoltage = "";
                String strSingleBasestationLocation = "";
//                String strOBD_PID = "";
//                String strOBD_Travel = "";
//                String strSerialNo = "";
//                String strIsSupportAndStatusCode = "";
//                String strAlarmBitExt = "";
//                String strExtStatusInfo = "";

                for (String ext : arrExtDataInfos) {
                    String startChar = ext.substring(0, 1);
                    switch (startChar) {
                        case "A"://GPS定位数据，34Bytes A1628212145497511136900061025191217
                            strGpsLocation = ext.substring(1).trim();
                            break;
                        case "B"://状态及报警编码，10Bytes  例：B0100000020
                            strStatusAndAlarmCode = ext.substring(1).trim();
                            break;
                        case "D"://汽车VIN码，N字节
                            strVin = ext.substring(1).trim();
                            break;
                        case "C":
                            strTotalMileage = ext.substring(1).trim();
                            break;
                        case "P": // P0460000026f87da9
                            strSingleBasestationLocation = ext.substring(1).trim();
                            break;
                        case "Y":
                            strExternalVoltage = ext.substring(1).trim();
                            break;
                        default:
                            break;
                    }
                }

                String commandType = functionTypeCode + functionItemKeyword;

                if ((!StringUtils.isBlank(strVin)) && (!StringUtils.equalsIgnoreCase(commandType, "AJ"))) {
                    //处理VIN码
                    handleVin(deviceNumber, strVin, vertx).setHandler(res -> {
                        if (res.succeeded()) {
                            logger.trace("Insert or Update Vin successfully.");
                        } else {
                            logger.error("Failed insert or update Vin .");
                        }
                    });
                }

                //外电压报警
                if(!StringUtils.isBlank(strExternalVoltage)){
                    handleExternalVoltage(deviceNumber, strExternalVoltage, vertx).setHandler(res -> {
                        if (res.succeeded()) {
                            logger.trace("handle external voltage successfully.");
                        } else {
                            logger.error("Failed external voltage.");
                        }
                    });
                }

                //总里程数
                float totalMileage = 0.00f;
                if (!StringUtils.isBlank(strTotalMileage)) {
                    //2节*秒
                    float mileageData;
                    try {
                        mileageData = (float) Long.parseLong(strTotalMileage, 16);
                    }
                    catch (Exception e){
                        mileageData = 0.0f;
                    }
                    //千米每小时
                    totalMileage = mileageData * 2 * 1.852f;
                }

                if (!StringUtils.isBlank(replyContent)) {// 终端答复数据包，答复Y
                    //*MG201868500027121029,YBI#
                    if (StringUtils.equalsIgnoreCase(replyContent, "Y")) {
                        handleSendDownReplyMessage(deviceNumber, commandType, vertx).setHandler(r -> {
                            if (r.succeeded()) {
                                logger.trace("handle senddown reply message successfully.");
                            } else {
                                logger.error("failed handle senddown reply message.", r.cause());
                            }
                        });
                    }
                } else {// 正常上传数据包，非终端答复数据包
                    switch (functionTypeCode) {
                        case "A"://上传状态类信息
                            switch (functionItemKeyword) {
                                case "A":
                                    break;
                                case "B": //登录信息，需要应答，"*MG20YAB#"
                                case "U": //上传短信或者电话查位置请求，"*MG20YAU#"
                                    // 处理状态及报警编码信息
                                    handleStatusAndAlarmCode(deviceNumber, strStatusAndAlarmCode, vertx);
                                    break;
                                case "H": //心跳包，需要应答，"*MG20YAH#"
                                    // 处理状态及报警编码信息
                                    handleStatusAndAlarmCode(deviceNumber, strStatusAndAlarmCode, vertx);

//                                    // 保存心跳包
//                                    Future<Boolean> heartbeatFut = handleHeartBeat(deviceNumber, vertx);
//                                    // 处理状态及报警编码信息
//                                    Future<Boolean> statusAndAlarmFut = handleStatusAndAlarmCode(deviceNumber, strStatusAndAlarmCode, vertx);
//                                    CompositeFuture.all(heartbeatFut, statusAndAlarmFut).setHandler(res -> {
//                                        if (res.succeeded()) {
//                                            logger.trace("update heartbeat and insert alarm data to redis successfully.");
//                                        } else {
//                                            logger.error("update heartbeat and insert alarm data to redis failed.");
//                                        }
//                                    });
                                    break;
                                case "J": //上传终端参数，"*MG20YAJ#"
                                case "W": //同步终端参数，"*MG20YAW#"
                                    break;
                            }
                            break;
                        case "B"://上传定位类信息
                            switch (functionItemKeyword) {
                                case "A": //定时回传的定位信息
                                    // 处理状态及报警编码信息
                                    Future<Boolean> statusAndAlarmFut = handleStatusAndAlarmCode(deviceNumber, strStatusAndAlarmCode, vertx);

                                    // 处理GPS数据
                                    Future<ObdDataEntity> entityBuilderFut = Future.future();
                                    logger.trace("strGpsLocation: " + strGpsLocation + ", strSingleBaseStationLocation: " + strSingleBasestationLocation);
                                    if(!StringUtils.isBlank(strGpsLocation)) { //正常定位
                                        entityBuilderFut = buildObdDataEntityBy(deviceNumber, "A", strGpsLocation, totalMileage, strExternalVoltage);
                                    }
                                    else if(!StringUtils.isBlank(strSingleBasestationLocation)){ //单基站定位
                                        entityBuilderFut = buildObdDataEntityBy(deviceNumber, "P", strSingleBasestationLocation, totalMileage, strExternalVoltage);
                                    }

                                    Future<Boolean> positionFut = Future.future();
                                    entityBuilderFut.compose(obdDataEntity -> {
                                        if(obdDataEntity != null) {
                                            //坐标系转换
                                            Gps bdGps = PositionUtils.WGS84_To_BD09(NumberUtils.toDouble(obdDataEntity.getLatitude()), NumberUtils.toDouble(obdDataEntity.getLongitude()));
                                            obdDataEntity.setbLatitude(String.valueOf(bdGps.getLat()));
                                            obdDataEntity.setbLongitude(String.valueOf(bdGps.getLon()));

                                            saveObdDataToRedisAndPublish(obdDataEntity, vertx).setHandler(positionFut.completer());
                                        }
                                        else {
                                            logger.trace("ObdDataEntity is null.");
                                            positionFut.complete(false);
                                        }
                                    }, positionFut);

                                    CompositeFuture.all(statusAndAlarmFut, positionFut).setHandler(res -> {
                                        if (res.succeeded()) {
                                            logger.trace("insert obddata successfully.");
                                        } else {
                                            logger.error("insert obddata failed.", res.cause());
                                        }
                                    });
                                    break;
                                case "Q": //IP列表上报
                                    logger.trace("IP List report.");
                                    break;
                            }
                            break;
                        default:
                            logger.trace("Unknown message type.");
                            break;
                    }
                }

                // 不管是什么数据包，都当作一次心跳
                handleHeartBeat(deviceNumber, vertx).setHandler(res -> {
                    if (res.succeeded()) {
                        logger.trace("update heartbeat time successfully.");
                    } else {
                        logger.error("Failed update heartbeat time.", res.cause());
                    }
                });

                if (StringUtils.equals(rspProperty, "1"))//需要回复
                {
                    VKT6MessageReplyer replyer = new VKT6MessageReplyer();
                    DataPackageReplyMessage replyMessage = new DataPackageReplyMessage();
                    replyMessage.setTerminalId(deviceNumber);
                    replyMessage.setFunctionType(functionTypeCode);
                    replyMessage.setFunctionItemKey(functionItemKeyword);
                    replyer.reply(vertx, replyMessage, writeHandlerID).setHandler(res -> {
                        if (res.succeeded()) {
                            logger.trace("reply to terminal successfully.");
                        } else {
                            logger.error("reply to terminal failed.");
                        }
                    });
                }
            }
            else {
                logger.warn("Message Header error. raw data: {}", rowData);
            }
        }
        else {
            logger.warn("Invalid message. length <= 5, raw data: {}", rowData);
        }

        logger.trace("--------------------Data parse end successfully------------------------");
    }

    private static String convertToHexFor(String rawData, String charsetName, String sectionSeparator, int byteSize){
        String convertedStr = rawData;
        try {
            if (rawData.contains(sectionSeparator)) {
                String[] arrTemp = rawData.split(sectionSeparator);
                if (arrTemp.length > 1) {
                    logger.trace("After " + sectionSeparator + ": " + arrTemp[1]);
                    String strWaitToConvert = StringUtil.substringByte(arrTemp[1], byteSize, charsetName);
                    logger.trace(strWaitToConvert);
                    convertedStr = arrTemp[0] + sectionSeparator + StringUtil.bytes2HexString(strWaitToConvert.getBytes(charsetName))
                            + StringUtil.substringByte(arrTemp[1],byteSize + 1, StringUtil.getStringByteLenths(arrTemp[1], charsetName) - byteSize, charsetName);
                }
            }
        }
        catch (Exception e){
            logger.error("Failed convert to hex string.");
        }
        return convertedStr;
    }

    public static void main(String[] args) {
        int uid = 5;
        String binaryStr = Integer.toBinaryString(uid);
        int m = uid % 2;
        double log = Math.log(2)/Math.log(2);
        String rightStr = StringUtils.right(binaryStr, (int)log);
        String uuidBinaryStr = binaryStr + rightStr;
        int uuid = Integer.parseInt(uuidBinaryStr, 2);
        int mm = uuid % 2;
        System.out.println("binaryStr: " + binaryStr + ", log: " + log + ", rightStr: " + rightStr + ", uuidBinaryStr: " + uuidBinaryStr);
        System.out.println("uid: " + uid + ", m: " + m + ", uuid: " + uuid + ", mm: " + mm);
    }

    private Future<Boolean> handleExternalVoltage(String deviceNumber, String strExternalVoltage, Vertx vertx) {
        Future<Boolean> result = Future.future();

        JsonObject jsonMessage = new JsonObject().put("imei", deviceNumber).put("externalVoltage", strExternalVoltage);
        vertx.eventBus().<JsonObject>send(ADDR_ExternalVoltage, jsonMessage);

        result.complete(true);

        return result;
    }

    private void sendDownNotify(Vertx vertx, String deviceNumber, String writeHandlerID) {
        JsonObject jsonMessage = new JsonObject().put("imei", deviceNumber).put("writeHandlerID", writeHandlerID);
        vertx.eventBus().<JsonObject>send(ADDR_SocketReceived, jsonMessage);
    }

    private Future<Boolean> handleSendDownReplyMessage(String deviceNumber, String commandType, Vertx vertx) {
        Future<Boolean> result = Future.future();

        DeviceCommandEntity deviceCommandEntity = new DeviceCommandEntity();
        deviceCommandEntity.setImei(deviceNumber);
        deviceCommandEntity.setCommandType(commandType);

        IDeviceCommandService<DeviceCommandEntity> deviceCommandService = new DeviceCommandServiceImpl(vertx);
        deviceCommandService.updateReplyStatus(deviceCommandEntity).setHandler(res -> {
           if(res.succeeded()){
               result.complete(res.result());
           }
           else {
               result.fail(res.cause());
           }
        });

        return result;
    }

    private Future<Boolean> handleHeartBeat(String deviceNumber, Vertx vertx) {
        Future<Boolean> result = Future.future();

        //心跳已经恢复，通知更新离线报警的状态
        vertx.eventBus().<String>send(ADDR_HeartbeatRestored, deviceNumber);

        HeartBeatEntity heartBeatEntity = new HeartBeatEntity();
        heartBeatEntity.setImei(deviceNumber);
        heartBeatEntity.setHeartbeatTime(new Date());
        heartBeatEntity.setHadOfflineAlarm(0);

        vertx.eventBus().<Buffer>send(ADDR_HEARTBEAT, Json.encodeToBuffer(heartBeatEntity));

        result.complete(true);
        return result;
    }

    private Future<Boolean> saveObdDataToRedisAndPublish(ObdDataEntity obdDataEntity, Vertx vertx){
        Future<Boolean> result = Future.future();

        if(obdDataEntity != null) {
            // 存储至redis
            RedisObdDataRepositoryImpl redisObdDataRepository = new RedisObdDataRepositoryImpl(vertx);
            Future<Boolean> redisFut = redisObdDataRepository.insert(obdDataEntity);


            Future<Boolean> sendFut = Future.future();
            vertx.eventBus().<Buffer>send(ADDR_ObdData, Json.encodeToBuffer(obdDataEntity));
            sendFut.complete(true);

//------------------------------------------------------------------------------------------
//            vertx.executeBlocking(publishToQueueFut -> {
//                try {
//                    Connection connection = MQClient.getConnection(vertx);
//                    Channel channel = connection.createChannel();
//
//                    channel.queueDeclare(OBDDATA_QUEUE_NAME, false, false, false, null);
//
//                    channel.basicPublish("", OBDDATA_QUEUE_NAME, null,
//                            Json.encodePrettily(obdDataEntity).getBytes("UTF-8"));
//
//                    channel.close();
//                    connection.close();
//                }
//                catch (Exception ex){
//                    logger.error(ex.getMessage(), ex);
//                }
//            }, publishRes -> {
//                if(publishRes.succeeded()){
//                    logger.trace("Publish obd data to mq: " + OBDDATA_QUEUE_NAME + " successfully.");
//                    sendFut.complete();
//                }
//                else {
//                    logger.error("Failed publish obd data to mq: " + OBDDATA_QUEUE_NAME + ", obddata: " + Json.encodePrettily(obdDataEntity), publishRes.cause());
//                    sendFut.fail(publishRes.cause());
//                }
//            });
//--------------------------------------------------------------------------------------
//            final JsonObject obddataJson = new JsonObject().put("body", Json.encodePrettily(obdDataEntity));
//            MQClient.rabbitMQClient(vertx, OBDDATA_QUEUE_NAME).setHandler(initRes -> {
//                if (initRes.succeeded()) {
//                    RabbitMQClient client = initRes.result();
//                    client.basicPublish("", OBDDATA_QUEUE_NAME, obddataJson, pubResult -> {
//                        if (pubResult.succeeded()) {
//                            logger.trace("Publish obd data to mq: " + OBDDATA_QUEUE_NAME + " successfully.");
//                            sendFut.complete(true);
//                        } else {
//                            logger.error("Failed publish obd data to mq: " + OBDDATA_QUEUE_NAME + ", obddata: " + obddataJson.encodePrettily(), pubResult.cause());
//                            sendFut.fail(pubResult.cause());
//                        }
//                    });
//                } else {
//                    sendFut.fail(initRes.cause());
//                }
//            });
//-----------------------------------------------------------------------------------------
            CompositeFuture.all(redisFut, sendFut).setHandler(res -> {
                if (res.succeeded()) {
                    logger.trace("save obd data to redis and send by event bus successfully.");
                    result.complete(true);
                } else {
                    logger.error("Failed save obd data to redis and send by event bus.", res.cause());
                    result.fail(res.cause());
                }
            });
        }
        else {
            result.fail("ObdDataEntity is null.");
        }
        return result;
    }

    private Future<Boolean> handleVin(String deviceNumber, String strVin, Vertx vertx) {
        Future<Boolean> result = Future.future();
        DeviceVehicleEntity dvEntity = new DeviceVehicleEntity();
        dvEntity.setImei(deviceNumber);
        dvEntity.setLinkedTime(new Date());

        //解析车辆VIN码
        parseVin(dvEntity, strVin);

        if(dvEntity != null){
            vertx.eventBus().<Buffer>send(ADDR_VIN, Json.encodeToBuffer(dvEntity));
            result.complete(true);
        }
        else {
            result.fail("dvEntity is null.");
        }

        return result;
    }

    private Future<Boolean> handleStatusAndAlarmCode(String deviceNumber, String strStatusAndAlarmCode, Vertx vertx)
    {
        Future<Boolean> result = Future.future();

        logger.trace("handle status and alarm code: " + strStatusAndAlarmCode);
        JsonObject jsonMessage = new JsonObject().put("imei", deviceNumber).put("statusAndAlarmCode", strStatusAndAlarmCode);
        vertx.eventBus().<JsonObject>send(ADDR_StatusAndAlarmCode, jsonMessage);

        result.complete(true);

        return result;
    }

    private void parseVin(DeviceVehicleEntity data, String strVin)
    {
        if(StringUtils.isBlank(strVin))
        {
            data.setVin("");
            return;
        }

        String isGetVinSuccess = strVin.substring(0, 1);
        if(isGetVinSuccess.contentEquals("0") || isGetVinSuccess.contentEquals("1"))
        {
            data.setVin("");
        }
        else if (isGetVinSuccess.contentEquals("2") || isGetVinSuccess.contentEquals("3"))
        {
            if (strVin.length() > 2)
            {
                data.setVin(strVin.substring(2));
            }
        }
    }

    private Future<ObdDataEntity> buildObdDataEntityBy(String deviceNumber, String locationType, String strLocation, float totalMileage, String strExternalVoltage)
    {
        Future<ObdDataEntity> result = Future.future();
        logger.trace("imei: " + deviceNumber + ", locationType: " + locationType + ", strLocation: " + strLocation);

        ObdDataEntity obdDataEntity = new ObdDataEntity();
        obdDataEntity.setImei(deviceNumber);
        obdDataEntity.setTotalMileage(totalMileage);
        obdDataEntity.setReceiveTime(new Date());

        if(!StringUtils.isBlank(strExternalVoltage)){
            Long externalVoltage = 0L;
            try{
                externalVoltage = Long.parseLong(strExternalVoltage);
            }
            catch (Exception e){
                logger.error("Failed parse external voltage to Long.", e);
            }

            obdDataEntity.setExternalVoltage(externalVoltage);
        }

        if(StringUtils.equals(locationType, "A")) { //1628262133766211128235260000191217

            obdDataEntity.setLocationType(1);
            String hhmmss = strLocation.substring(0, 2) + ":" + strLocation.substring(2, 4) + ":" + strLocation.substring(4, 6);
            String latitude = strLocation.substring(6, 14);
            String longitude = strLocation.substring(14, 23);
            String locationFlag = strLocation.substring(23, 24);
            String speed = strLocation.substring(24, 26);
            String direction = strLocation.substring(26, 28);
            String yyMMdd = strLocation.substring(32, 34) + "-" + strLocation.substring(30, 32) + "-" + strLocation.substring(28, 30);
            logger.trace("report time: " + yyMMdd + " " + hhmmss);
            Date reportTime;
            try {
                reportTime = DateUtils.parseDate(yyMMdd + " " + hhmmss, "yy-MM-dd HH:mm:ss");
                logger.trace("parsed report time: " + reportTime);
                obdDataEntity.setReportTime(reportTime);
            }
            catch (Exception e){
                logger.error(e.getMessage(), e);
                obdDataEntity.setReportTime(new Date());
            }

            BigDecimal lat = NumberUtils.createBigDecimal(latitude.substring(0, 2))
                    .add(NumberUtils.createBigDecimal(latitude.substring(2, 4) + "." + latitude.substring(4, 8))
                            .divide(NumberUtils.createBigDecimal("60"), 10, BigDecimal.ROUND_UP));
            obdDataEntity.setLatitude(String.valueOf(lat));
            BigDecimal lon = NumberUtils.createBigDecimal(longitude.substring(0, 3))
                    .add(NumberUtils.createBigDecimal(longitude.substring(3, 5) + "." + longitude.substring(5, 9))
                            .divide(NumberUtils.createBigDecimal("60"), 10, BigDecimal.ROUND_UP));
            obdDataEntity.setLongitude(String.valueOf(lon));

            //单位：km/h
            obdDataEntity.setSpeed((float) NumberUtils.toShort(speed) * 2 * 1.852f);

            //单位：度
            obdDataEntity.setOrientation(NumberUtils.toFloat(direction) * 10f);

            if (StringUtils.equals(locationFlag, "0") || StringUtils.equals(locationFlag, "2")
                    || StringUtils.equals(locationFlag, "4") || StringUtils.equals(locationFlag, "6")) {
                obdDataEntity.setIsValid(1);
            } else if (StringUtils.equals(locationFlag, "1") || StringUtils.equals(locationFlag, "3")
                    || StringUtils.equals(locationFlag, "5") || StringUtils.equals(locationFlag, "7")) {
                obdDataEntity.setIsValid(2);
            } else if (StringUtils.equals(locationFlag, "F")) {
                obdDataEntity.setIsValid(2);
            }

            result.complete(obdDataEntity);
        }
        else if(StringUtils.equals(locationType, "P")){ //0460000026f87da9
            obdDataEntity.setLocationType(2);

            result.complete(null);

//            String MCC = strLocation.substring(0, 4);
//            String MNC = strLocation.substring(4, 8);
//            String LAC = strLocation.substring(8, 12);
//
//            //http://minigps.net/cw?x=mcc-mnc-lac1-cellid1-signal1-lac2-cellid2-signal2-lac3-cellid3-signal3&p&mt&needaddress=0
//
//            String configBaseStationServiceHost = config.getJsonObject("base-station-service").getString("host");
//            String configBaseStationServiceUri = config.getJsonObject("base-station-service").getString("uri");
//
//            StringBuilder xQueryParamBuilder = new StringBuilder();
//            xQueryParamBuilder.append(MCC);
//            xQueryParamBuilder.append("-" + MNC);
//            xQueryParamBuilder.append("-" + LAC);
//
//            String signal = "96";
//            String CellID, subsidiaryAreaCode1, subsidiaryCellID1, subsidiaryAreaCode2, subsidiaryCellID2;
//            if(strLocation.length() >= 34){
//                CellID = strLocation.substring(12, 16);
//                subsidiaryAreaCode1 = strLocation.substring(17, 21);
//                subsidiaryCellID1 = strLocation.substring(21, 25);
//                subsidiaryAreaCode2 = strLocation.substring(26, 30);
//                subsidiaryCellID2 = strLocation.substring(30);
//
//                xQueryParamBuilder.append("-" + CellID);
//                xQueryParamBuilder.append("-" + signal);
//                xQueryParamBuilder.append("-" + subsidiaryAreaCode1);
//                xQueryParamBuilder.append("-" + subsidiaryCellID1);
//                xQueryParamBuilder.append("-" + signal);
//                xQueryParamBuilder.append("-" + subsidiaryAreaCode2);
//                xQueryParamBuilder.append("-" + subsidiaryCellID2);
//                xQueryParamBuilder.append("-" + signal);
//            }
//            else if(strLocation.length() >= 25){
//                CellID = strLocation.substring(12, 16);
//                subsidiaryAreaCode1 = strLocation.substring(17, 21);
//                subsidiaryCellID1 = strLocation.substring(21);
//
//                xQueryParamBuilder.append("-" + CellID);
//                xQueryParamBuilder.append("-" + signal);
//                xQueryParamBuilder.append("-" + subsidiaryAreaCode1);
//                xQueryParamBuilder.append("-" + subsidiaryCellID1);
//                xQueryParamBuilder.append("-" + signal);
//            }
//            else {
//                CellID = strLocation.substring(12);
//
//                xQueryParamBuilder.append("-" + CellID);
//                xQueryParamBuilder.append("-" + signal);
//            }
//
//            WebClientOptions options = new WebClientOptions();
//            options.setKeepAlive(true);
//            WebClient client = WebClient.create(vertx, options);
//            client.get(configBaseStationServiceHost, configBaseStationServiceUri)
//                    .addQueryParam("x", xQueryParamBuilder.toString())
//                    .addQueryParam("p", "1")
//                    .addQueryParam("mt", "0")
//                    .addQueryParam("needaddress", "0")
//                    .putHeader("Content-Type", "application/json")
//                    .send(ar -> {
//                        if(ar.succeeded()){
//                            HttpResponse<Buffer> response = ar.result();
//
//                            result.complete(obdDataEntity);
//                        }
//                        else {
//                            logger.error("Base Station Service is unavailable.", ar.cause());
//                            result.fail(ar.cause());
//                        }
//                    });
        }

        return result;
    }
}
