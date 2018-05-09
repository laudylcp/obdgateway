package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.entity.ObdDataEntity;
import com.gzfns.obdpps.mq.MQClient;
import com.gzfns.obdpps.service.IObdDataService;
import com.gzfns.obdpps.service.ObdDataServiceImpl;
import com.rabbitmq.client.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataConsumeVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(DataConsumeVerticle.class);

    private static final String OBDDATA_QUEUE_NAME = "queue-obd-data";

    private static int mysqlBatchSize;

    private static final IObdDataService obdDataService = new ObdDataServiceImpl();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();
        mysqlBatchSize = config().getJsonObject("mysql").getInteger("batch_size", 5);

        vertx.executeBlocking(startConsumeFut -> {
            try {
                startConsumeOBDData();
                startConsumeFut.complete();
            }
            catch (Exception ex){
                startConsumeFut.fail(ex.getCause());
            }
        }, res -> {
            if(res.succeeded()){
                logger.info("DataConsumeVerticle start successfully.");
                startFuture.complete();
            }
            else {
                logger.info("Failed DataConsumeVerticle start", res.cause());
                startFuture.fail(res.cause());
            }
        });
    }

    private void startConsumeOBDData(){
        logger.trace("enter startConsumeOBDData.");
        try {
            Connection connection = MQClient.getConnection(vertx);

            Channel channel = connection.createChannel();

            channel.queueDeclare(OBDDATA_QUEUE_NAME, false, false, false, null);
            channel.basicQos(1);

            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String obdDataEntityJsonStr = new String(body, "UTF-8");
                    logger.trace("get obd data from mq: " + OBDDATA_QUEUE_NAME + ", obdDataEntityJsonStr: " + obdDataEntityJsonStr);

                    ObdDataEntity obdDataEntity = Json.decodeValue(obdDataEntityJsonStr, ObdDataEntity.class);

                    long messageCount = channel.messageCount(OBDDATA_QUEUE_NAME);
                    if(messageCount > 0){
                        //根据mysqlBatchSize判断是否写入数据库
                        obdDataService.batchSave(vertx, obdDataEntity, mysqlBatchSize, false).setHandler(batchRes -> {
                            if(batchRes.succeeded()){
                                logger.trace("obdDataService.batchSave successfully, startSaving: false");
                            }
                            else {
                                logger.error("Failed obdDataService.batchSave, startSaving: false");
                            }
                        });
                    }
                    else {
                        //立即存入数据库
                        obdDataService.batchSave(vertx, obdDataEntity, mysqlBatchSize, true).setHandler(batchRes -> {
                            if(batchRes.succeeded()){
                                logger.trace("obdDataService.batchSave successfully, startSaving: true");
                            }
                            else {
                                logger.error("Failed obdDataService.batchSave, startSaving: true");
                            }
                        });
                    }

                    channel.basicAck(envelope.getDeliveryTag(), false);
                    logger.trace("obddata ack successfully.");
                }
            };

            channel.basicConsume(OBDDATA_QUEUE_NAME, false, consumer);
            logger.trace("consume message from mq: " + OBDDATA_QUEUE_NAME);

//            channel.close();
//            connection.close();
        }
        catch (Exception ex){
            logger.error("startConsumeOBDData: " + ex.getMessage(), ex);
        }
    }
}
