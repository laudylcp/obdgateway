package com.gzfns.obdpps;

import com.gzfns.obdpps.util.JsonConfigUtil;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.*;
import io.vertx.core.impl.Args;
import io.vertx.core.impl.VertxFactoryImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.VertxFactory;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class ApplicationStart {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationStart.class);

    private static JsonObject config = null;

    public static void main(String[] sargs){

        Args args = new Args(sargs);
        String profileArg = args.map.get("-profile");
        if(StringUtils.isBlank(profileArg)){
            profileArg = System.getProperty("profile", "dev");
        }

        final String profile = profileArg;

        config = JsonConfigUtil.getConfig(ApplicationStart.class, "config/" + profile + "/config-" + profile + ".json");

        loadLog4j2Configuration(ApplicationStart.class, profile);

        VertxFactory factory = new VertxFactoryImpl();
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        VertxOptions options = new VertxOptions().setClusterManager(new HazelcastClusterManager(hazelcastInstance));
        factory.clusteredVertx(options, createVertxRes -> {
            if(createVertxRes.succeeded()){
                final Vertx vertx = createVertxRes.result();

                final DeploymentOptions tcpDeploymentOptions = tcpDeployOptions();
                tcpDeploymentOptions.setConfig(config);

                Future<String> tcpFut = Future.future();
                vertx.deployVerticle("com.gzfns.obdpps.verticle.ReceiveDataVerticle", tcpDeploymentOptions, res -> {
                    if (res.succeeded()) {
                        logger.info("ReceiveDataVerticle had deployed. The deployment id is: "
                                + res.result() + ", using profile: " + profile);
                        tcpFut.complete(res.result());
                    } else {
                        logger.error("ReceiveDataVerticle deploy failed! using profile: " + profile, res.cause());
                        tcpFut.fail(res.cause());
                    }
                });

                Future<String> httpFut = Future.future();
                final DeploymentOptions httpDeploymentOptions = httpDeployOptions();
                httpDeploymentOptions.setConfig(config);

                vertx.deployVerticle("com.gzfns.obdpps.verticle.ReceiveCommandVerticle", httpDeploymentOptions, res -> {
                    if(res.succeeded()) {
                        logger.info("ReceiveCommandVerticle had deployed. The deployment id is: "
                                + res.result() + ", using profile: " + profile);
                        httpFut.complete(res.result());
                    }
                    else {
                        logger.error("ReceiveCommandVerticle deploy failed! using profile: " + profile, res.cause());
                        httpFut.fail(res.cause());
                    }
                });

//                Future<String> mqconsumeFut = Future.future();
//                final DeploymentOptions mqconsumerDeployOptions = mqconsumerDeployOptions();
//                mqconsumerDeployOptions.setConfig(config);
//                vertx.deployVerticle("com.gzfns.obdpps.verticle.QueueConsumeVerticle", mqconsumerDeployOptions, res -> {
//                    if(res.succeeded()) {
//                        logger.info("QueueConsumeVerticle had deployed. The deployment id is: "
//                                + res.result() + ", using profile: " + profile);
//                        mqconsumeFut.complete(res.result());
//                    }
//                    else {
//                        logger.error("QueueConsumeVerticle deploy failed! using profile: " + profile, res.cause());
//                        mqconsumeFut.fail(res.cause());
//                    }
//                });

                Future<String> locationServiceFut = Future.future();
                final DeploymentOptions locationServiceDeployOptions = locationServiceDeployOptions();
                locationServiceDeployOptions.setConfig(config);
                vertx.deployVerticle("com.gzfns.obdpps.verticle.LocationServiceVerticle", locationServiceDeployOptions, res -> {
                    if(res.succeeded()) {
                        logger.info("LocationServiceVerticle had deployed. The deployment id is: "
                                + res.result() + ", using profile: " + profile);
                        locationServiceFut.complete(res.result());
                    }
                    else {
                        logger.error("LocationServiceVerticle deploy failed! using profile: " + profile, res.cause());
                        locationServiceFut.fail(res.cause());
                    }
                });

                Future<String> alarmServiceFut = Future.future();
                final DeploymentOptions alarmServiceDeployOptions = alarmServiceDeployOptions();
                alarmServiceDeployOptions.setConfig(config);
                vertx.deployVerticle("com.gzfns.obdpps.verticle.AlarmServiceVerticle", alarmServiceDeployOptions, res -> {
                    if(res.succeeded()) {
                        logger.info("AlarmServiceVerticle had deployed. The deployment id is: "
                                + res.result() + ", using profile: " + profile);
                        alarmServiceFut.complete(res.result());
                    }
                    else {
                        logger.error("AlarmServiceVerticle deploy failed! using profile: " + profile, res.cause());
                        alarmServiceFut.fail(res.cause());
                    }
                });

                Future<String> commonServiceFut = Future.future();
                final DeploymentOptions commonServiceDeployOptions = commonServiceDeployOptions();
                commonServiceDeployOptions.setConfig(config);
                vertx.deployVerticle("com.gzfns.obdpps.verticle.CommonServiceVerticle", commonServiceDeployOptions, res -> {
                    if(res.succeeded()) {
                        logger.info("CommonServiceVerticle had deployed. The deployment id is: "
                                + res.result() + ", using profile: " + profile);
                        commonServiceFut.complete(res.result());
                    }
                    else {
                        logger.error("CommonServiceVerticle deploy failed! using profile: " + profile, res.cause());
                        commonServiceFut.fail(res.cause());
                    }
                });

                CompositeFuture.all(tcpFut, httpFut, locationServiceFut, alarmServiceFut, commonServiceFut).setHandler(result -> {
                    if(result.succeeded()){
                        logger.info("All verticle had deployed successfully.");
                    }
                    else {
                        logger.error("Failed deploy all verticle.", result.cause());
                    }
                });
            }
            else {
                logger.error("Failed create clustered Vertx instance.", createVertxRes.cause());
            }
        });
    }

    private static void loadLog4j2Configuration(Class clazz, String profile){
        try {
            InputStream resourceAsStream = clazz.getClassLoader().getResourceAsStream("config/" + profile + "/log4j2-" + profile + ".xml");
            if (resourceAsStream == null) {
                throw new IllegalStateException("Cannot find log4j2-" + profile + ".xml on classpath");
            }

            ConfigurationSource source = new ConfigurationSource(resourceAsStream);
            LoggerContext context = Configurator.initialize(null, source);
            XmlConfiguration xmlConfig = new XmlConfiguration(context,source);
            context.start(xmlConfig);

            System.out.println("Load log4j2 config from: log4j2-" + profile + ".xml successfully.");

        } catch (Exception e) {
            System.out.println("Failed load log4j2 config from: log4j2-" + profile + ".xml");
            e.printStackTrace();
        }
    }

    private static DeploymentOptions tcpDeployOptions(){
        final DeploymentOptions options = new DeploymentOptions();
        //options.setHa(false);
        options.setInstances(config.getJsonObject("tcpServer").getInteger("instances"));
        //options.setWorker(false);
        //options.setMultiThreaded(false);
        return options;

    }

    private static DeploymentOptions httpDeployOptions(){
        final DeploymentOptions options = new DeploymentOptions();
        //options.setHa(false);
        options.setInstances(config.getJsonObject("httpServer").getInteger("instances"));
        //options.setWorker(false);
        //options.setMultiThreaded(false);
        return options;

    }

//    private static DeploymentOptions mqconsumerDeployOptions(){
//        final DeploymentOptions options = new DeploymentOptions();
//        //options.setHa(false);
//        options.setInstances(config.getJsonObject("rabbitmq").getInteger("consumer.instances"));
//        //options.setWorker(false);
//        //options.setMultiThreaded(false);
//        return options;
//
//    }

    private static DeploymentOptions locationServiceDeployOptions(){
        final DeploymentOptions options = new DeploymentOptions();
        //options.setHa(false);
        options.setInstances(config.getJsonObject("location-service").getInteger("instances"));
        options.setWorker(true);
        //options.setMultiThreaded(false);
        return options;

    }

    private static DeploymentOptions alarmServiceDeployOptions(){
        final DeploymentOptions options = new DeploymentOptions();
        //options.setHa(false);
        options.setInstances(config.getJsonObject("alarm-service").getInteger("instances"));
        options.setWorker(true);
        //options.setMultiThreaded(false);
        return options;

    }

    private static DeploymentOptions commonServiceDeployOptions(){
        final DeploymentOptions options = new DeploymentOptions();
        //options.setHa(false);
        options.setInstances(config.getJsonObject("common-service").getInteger("instances"));
        options.setWorker(true);
        //options.setMultiThreaded(false);
        return options;

    }
}
