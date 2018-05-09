package com.gzfns.obdpps.mq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQClient {

    private static final Logger logger = LoggerFactory.getLogger(MQClient.class);

    private static RabbitMQClient rabbitClient;

    public static Connection getConnection(Vertx vertx) throws Exception{
        final JsonObject config = vertx.getOrCreateContext().config().getJsonObject("rabbitmq");
        logger.trace("rabbitmq config: " + config.encodePrettily());

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getString("host", "localhost"));
        factory.setPort(config.getInteger("port", 5672));
        factory.setUsername(config.getString("user", "guest"));
        factory.setPassword(config.getString("password", "guest"));

        Connection connection = factory.newConnection();

        logger.trace("get rabbitmq connection successfully.");

        return connection;
    }

    public static Future<RabbitMQClient> rabbitMQClient(Vertx vertx, String queueName){
        Future<RabbitMQClient> result = Future.future();

        if(ObjectUtils.equals(rabbitClient, null)) {

            final JsonObject config = vertx.getOrCreateContext().config().getJsonObject("rabbitmq");
            logger.trace("rabbitmq config: " + config.encodePrettily());

            RabbitMQOptions options = new RabbitMQOptions();
            options.setUser(config.getString("user", "guest"));
            options.setPassword(config.getString("password", "guest"));
            options.setHost(config.getString("host", "localhost"));
            options.setPort(config.getInteger("port", 5672));
            //options.setVirtualHost("/");

//        options.setConnectionTimeout(6000); // in milliseconds
//        options.setRequestedHeartbeat(60); // in seconds
//        options.setHandshakeTimeout(6000); // in milliseconds
//        options.setRequestedChannelMax(5);
//        options.setNetworkRecoveryInterval(500); // in milliseconds
//        options.setAutomaticRecoveryEnabled(true);

            rabbitClient = RabbitMQClient.create(vertx, options);

            Future<Void> startFut = Future.future();
            rabbitClient.start(startRes -> {
                if (startRes.succeeded()) {
                    logger.trace("rabbitmq client started successfully. isOpenChannel: " + rabbitClient.isOpenChannel()
                            + ", isConnected: " + rabbitClient.isConnected());
                    startFut.complete();
                } else {
                    logger.error("Failed starting rabbitmq client.", startRes.cause());
                    startFut.fail(startRes.cause());
                }
            });
            startFut.compose(start -> {
                Future<Void> declareFut = Future.future();
                rabbitClient.queueDeclare(queueName, false, false, false, null, queueDeclareRes -> {
                    if (queueDeclareRes.succeeded()) {
                        logger.info("queueName: " + queueName + " declare successfully.");
                        declareFut.complete();

                    } else {
                        logger.error("Failed declare queueName: " + queueName, queueDeclareRes.cause());
                        declareFut.fail(queueDeclareRes.cause());

                    }
                });
                return declareFut;
            }).compose(declare -> {
                rabbitClient.basicQos(1, basicQosRes -> {
                    if (basicQosRes.succeeded()) {
                        logger.trace("set basic Qos=1 successfully.");
                        result.complete(rabbitClient);
                    } else {
                        logger.error("Failed set basic Qos=1.", basicQosRes.cause());
                        result.fail(basicQosRes.cause());
                    }
                });
            }, result);
        }
        else {
            result.complete(rabbitClient);
        }

        return result;
    }

}
