package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.entity.ObdDataEntity;
import com.gzfns.obdpps.mq.MQClient;
import com.gzfns.obdpps.service.IObdDataService;
import com.gzfns.obdpps.service.ObdDataServiceImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueConsumeVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(QueueConsumeVerticle.class);

    private static final String OBDDATA_QUEUE_NAME = "queue-obd-data";

    private static final String OBDDATA_EVENTBUS_ADDRESS = "eventbus-obd-data";

    private static int mysqlBatchSize;

    private static final IObdDataService obdDataService = new ObdDataServiceImpl();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();
        mysqlBatchSize = config().getJsonObject("mysql").getInteger("batch_size", 5);

        MQClient.rabbitMQClient(vertx, OBDDATA_QUEUE_NAME).setHandler(initRes -> {
            if(initRes.succeeded()){
                RabbitMQClient client = initRes.result();
                logger.trace("init rabbitmq client successfully.");

                startConsumeOBDData(client);
                startFuture.complete();
            }
            else {
                logger.error("Failed init rabbitmq client.", initRes.cause());
                startFuture.fail(initRes.cause());
            }
        });
    }

    private void startConsumeOBDData(RabbitMQClient client){
        vertx.eventBus().consumer(OBDDATA_EVENTBUS_ADDRESS, msg -> {
            JsonObject data = (JsonObject)msg.body();
            logger.trace("Got message: " + data.encodePrettily());
            String jsonEntity = data.getString("body");
            logger.trace("get obd data from mq: " + OBDDATA_QUEUE_NAME + ", jsonObdData: " + jsonEntity);
            ObdDataEntity obdDataEntity = Json.decodeValue(jsonEntity, ObdDataEntity.class);

            client.messageCount(OBDDATA_QUEUE_NAME, countRes -> {
                if(countRes.succeeded()){
                    logger.trace("get count success, " + countRes.result().encodePrettily());
                    Future<Void> batchInsertFut = Future.future();
                    Long messageCount = countRes.result().getLong("messageCount");
                    if(messageCount > 0){
                        //根据mysqlBatchSize判断是否写入数据库
                        obdDataService.batchSave(vertx, obdDataEntity, mysqlBatchSize, false).setHandler(batchRes -> {
                            if(batchRes.succeeded()){
                                batchInsertFut.complete();
                            }
                            else {
                                batchInsertFut.fail(batchRes.cause());
                            }
                        });
                    }
                    else {
                        //立即存入数据库
                        obdDataService.batchSave(vertx, obdDataEntity, mysqlBatchSize, true).setHandler(batchRes -> {
                            if(batchRes.succeeded()){
                                batchInsertFut.complete();
                            }
                            else {
                                batchInsertFut.fail(batchRes.cause());
                            }
                        });
                    }

                    batchInsertFut.compose(v -> {
                        Future<Void> ackFut = Future.future();
                        //ack
                        Long deliveryTag = data.getLong("deliveryTag");
                        client.basicAck(deliveryTag, false, asyncResult -> {
                            if (asyncResult.succeeded()) {
                                logger.trace("obddata ack successfully.");
                                ackFut.complete();
                            } else {
                                logger.error("Failed ack: " + deliveryTag);
                                ackFut.fail(asyncResult.cause());
                            }
                        });

                        return ackFut;
                    });
                }
                else {
                    logger.trace("Failed count.", countRes.cause());
                }
            });
        });

        client.basicConsume(OBDDATA_QUEUE_NAME, OBDDATA_EVENTBUS_ADDRESS, false, consumeResult -> {
            if(consumeResult.succeeded()){
                logger.trace("consume1 message from mq: " + OBDDATA_QUEUE_NAME + " to eventbus: " + OBDDATA_EVENTBUS_ADDRESS);
            }
            else {
                logger.error("Failed basicConsume1.", consumeResult.cause());
            }
        });

    }
}
