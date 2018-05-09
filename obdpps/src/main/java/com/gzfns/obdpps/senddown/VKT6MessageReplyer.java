package com.gzfns.obdpps.senddown;

import com.gzfns.obdpps.command.vkt6.DataPackageReplyMessage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;


public class VKT6MessageReplyer {

    private static final Logger logger = LoggerFactory.getLogger(VKT6MessageReplyer.class);

    public Future<Boolean> reply(Vertx vertx, DataPackageReplyMessage replyMessage, String writeHandlerID) {
        Future<Boolean> result = Future.future();

        String terminalId = replyMessage.terminalId();
        final String commandBody = replyMessage.body();
        byte[] commandBuffer = commandBody.getBytes(Charset.forName("ASCII"));
        Buffer buffer = Buffer.buffer(commandBuffer);

        vertx.eventBus().<Buffer>send(writeHandlerID, buffer);
        logger.trace("reply message: " + commandBody + " to terminalId: " + terminalId);

        result.complete(true);

        return result;
    }
}
