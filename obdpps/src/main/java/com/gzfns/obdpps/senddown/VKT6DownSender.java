package com.gzfns.obdpps.senddown;

import com.gzfns.obdpps.entity.DeviceCommandEntity;
import com.gzfns.obdpps.service.DeviceCommandServiceImpl;
import com.gzfns.obdpps.service.IDeviceCommandService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


public class VKT6DownSender<T> implements IDownSender<T> {

    private static final Logger logger = LoggerFactory.getLogger(VKT6DownSender.class);

    private static Vertx vertx;

    public VKT6DownSender(Vertx vertx){
        this.vertx = vertx;
    }

    @Override
    public Future<Boolean> senddown(List<T> commandList, String writeHandlerID) {
        Future<Boolean> result = Future.future();

        List<Future> futures = new ArrayList<>();
        IDeviceCommandService<DeviceCommandEntity> deviceCommandService = new DeviceCommandServiceImpl(vertx);
        for (T command : commandList) {
            DeviceCommandEntity deviceCommand = (DeviceCommandEntity)command;
            String terminalId = deviceCommand.getImei();
            String commandBody = deviceCommand.getCommandContent();
            byte[] commandBuffer = commandBody.getBytes(Charset.forName("ASCII"));
            Buffer buffer = Buffer.buffer(commandBuffer);

            logger.trace("send down command: " + commandBody + " to terminalId: " + terminalId);
            vertx.eventBus().<Buffer>send(writeHandlerID, buffer);

            //成功下发一条指令后，就更新该条指令的下发状态，并从redis中删除
            Future<Boolean> updateFut = deviceCommandService.updateSendStatus(deviceCommand);
            futures.add(updateFut);
        }

        CompositeFuture.all(futures).setHandler(sendDownRes -> {
            if(sendDownRes.succeeded()){
                logger.trace("send down command list and update send status to mysql and delete from redis successfully.");
                result.complete(true);
            }
            else {
                logger.error("Failed send down command list and update send status to mysql and delete from redis.", sendDownRes.cause());
                result.fail(sendDownRes.cause());
            }
        });

        return result;
    }

    @Override
    public Future<Boolean> senddown(T command, String writeHandlerID) {
        List<T> commandList = new ArrayList<>();
        commandList.add(command);
        return senddown(commandList, writeHandlerID);
    }
}
