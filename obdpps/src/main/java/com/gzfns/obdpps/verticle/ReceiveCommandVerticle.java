package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.command.vkt6.SendDownCommand;
import com.gzfns.obdpps.entity.DeviceCommandEntity;
import com.gzfns.obdpps.senddown.IDownSender;
import com.gzfns.obdpps.senddown.VKT6DownSender;
import com.gzfns.obdpps.service.DeviceCommandServiceImpl;
import com.gzfns.obdpps.service.IDeviceCommandService;
import com.gzfns.obdpps.util.CommandUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

public class ReceiveCommandVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(ReceiveCommandVerticle.class);

    private static final String ADDR_SocketReceived = "address_socketReceived";

    @Override
    public void start(Future<Void> future) throws Exception {
        super.start();

        socketReceivedNotified();

        startHttpServer(future);
    }

    private void socketReceivedNotified(){
        vertx.eventBus().<JsonObject>consumer(ADDR_SocketReceived, message -> {
            JsonObject jsonMessage = message.body();
            final String imei = jsonMessage.getString("imei");
            final String writeHandlerID = jsonMessage.getString("writeHandlerID");
            getCommandList(imei).setHandler(getListRes -> {
                if(getListRes.succeeded()){
                    List<DeviceCommandEntity> list = getListRes.result();
                    if(list.size() > 0) {
                        IDownSender<DeviceCommandEntity> downSender = new VKT6DownSender(vertx);
                        downSender.senddown(list, writeHandlerID).setHandler(r -> {
                            if (r.succeeded() && r.result()) {//下发成功
                                logger.trace("send down command list successfully.");
                            } else {
                                logger.error("Failed send down command list.");
                            }
                        });
                    }
                    else {
                        logger.trace("Get command list successfully. list size: 0.");
                    }
                }
                else {
                    logger.error("Failed get command list from redis.");
                }
            });
        });
    }

    private Future<List<DeviceCommandEntity>> getCommandList(String imei){
        Future<List<DeviceCommandEntity>> result = Future.future();

        IDeviceCommandService<DeviceCommandEntity> deviceCommandService = new DeviceCommandServiceImpl(vertx);
        DeviceCommandEntity where = new DeviceCommandEntity();
        where.setImei(imei);
        deviceCommandService.getRedisList(where).setHandler(res -> {
            if(res.succeeded()){
                List<DeviceCommandEntity> commandList = res.result();
                logger.trace("Get command list from redis successfully.");
                result.complete(commandList);
            }
            else {
                logger.error("Failed get command list from redis.", res.cause());
                result.fail(res.cause());
            }
        });

        return result;
    }

    private void startHttpServer(Future<Void> future) throws Exception{

        Router router = Router.router(vertx);
        // CORS support
        Set<String> allowHeaders = new HashSet<>();
        allowHeaders.add("x-requested-with");
        allowHeaders.add("Access-Control-Allow-Origin");
        allowHeaders.add("origin");
        allowHeaders.add("Content-Type");
        allowHeaders.add("accept");
        Set<HttpMethod> allowMethods = new HashSet<>();
        allowMethods.add(HttpMethod.GET);
        allowMethods.add(HttpMethod.POST);
//        allowMethods.add(HttpMethod.DELETE);
//        allowMethods.add(HttpMethod.PATCH);

        router.route().handler(BodyHandler.create());
        router.route().handler(CorsHandler.create("*")
                .allowedHeaders(allowHeaders)
                .allowedMethods(allowMethods));

        // route that handle receive command json array.
        router.route("/sendcommand/timingback/").method(HttpMethod.POST)
                .consumes("application/json")
                .produces("application/json")
                .handler(this::handleTimingBackCommand)
                .failureHandler(this::failureSendCommand);

        int port = config().getJsonObject("httpServer").getInteger("port", 8080);
        String address = config().getJsonObject("httpServer").getString("address", "0.0.0.0");
        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(port, address, result -> {
                    if (result.succeeded()) {
                        logger.info("Http server had listened on " + address + ":" + port);
                        future.complete();
                    }
                    else {
                        logger.error("http server start failed.", result.cause());
                        future.fail(result.cause());
                    }
                });
    }

    private void handleTimingBackCommand(RoutingContext routingContext) {
        JsonArray commandArray = routingContext.getBodyAsJsonArray();
        // [{"imei":"871600005491029", "interval":120,"sendtimes":1},{"imei":"771620005491028", "interval":120,"sendtimes":1}]
        logger.trace("received command array: " + commandArray.encodePrettily());
        try {
            List<DeviceCommandEntity> deviceCommandEntityList = new ArrayList<>();
            for (Object command : commandArray) {
                JsonObject timingBackJson = JsonObject.mapFrom(command);
                String interval = CommandUtil.get4BytesHexString(timingBackJson.getInteger("interval"));
                String sendtimes = CommandUtil.get4BytesHexString(timingBackJson.getInteger("sendtimes"));

                logger.trace("interval: " + interval + ", sendtimes: " + sendtimes);

                String imei = timingBackJson.getString("imei","000000000000000");

                SendDownCommand timingBackCommand = new SendDownCommand();
                timingBackCommand.setTerminalId(imei);
                timingBackCommand.setStoreProperty("1");
                timingBackCommand.setReplyProperty("1");
                timingBackCommand.setFunctionType("B");
                timingBackCommand.setFunctionItemKey("I");
                timingBackCommand.setCommandData(interval + sendtimes);

                DeviceCommandEntity deviceCommandEntity = new DeviceCommandEntity();
                deviceCommandEntity.setImei(imei);
                deviceCommandEntity.setCommandType("BI");
                deviceCommandEntity.setCommandContent(timingBackCommand.body());
                deviceCommandEntity.setAddTime(new Date());
                deviceCommandEntity.setIsSend(false);
                deviceCommandEntity.setSendTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1970-01-01 00:00:00"));
                deviceCommandEntity.setIsReply(false);
                deviceCommandEntity.setReplyTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1970-01-01 00:00:00"));
                deviceCommandEntityList.add(deviceCommandEntity);
            }

            //存储至redis、mysql
            IDeviceCommandService<DeviceCommandEntity> deviceCommandService = new DeviceCommandServiceImpl(vertx);
            deviceCommandService.saveList(deviceCommandEntityList).setHandler(resultHandler(routingContext, s -> {
                logger.trace("command had saved to db, After the terminal connected to server, begin sending down to terminal.");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .setChunked(true)
                        .end(new JsonObject()
                                .put("message","指令成功存储，对应设备连接上服务器后，将会下发指令至该设备.")
                                .put("status",true)
                                .toBuffer());

            }));
        }
        catch (Exception ex) {
            sendError(400, routingContext.response());
        }
    }

    private void failureSendCommand(RoutingContext routingContext) {
        logger.trace("routing failed: " + routingContext.failure().getMessage());
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json");
        response.setChunked(true);
        response.end(new JsonObject().put("message", "Routing error! Can't find corresponding command handler.")
                .put("status", false)
                .toBuffer());
    }

    private <T> Handler<AsyncResult<T>> resultHandler(RoutingContext context, Consumer<T> consumer) {
        return res -> {
            if (res.succeeded()) {
                consumer.accept(res.result());
            } else {
                serviceUnavailable(context);
            }
        };
    }

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

    private void notFound(RoutingContext context) {
        context.response().setStatusCode(404).end();
    }

    private void badRequest(RoutingContext context) {
        context.response().setStatusCode(400).end();
    }

    private void serviceUnavailable(RoutingContext context) {
        context.response().setStatusCode(503).end();
    }

}
