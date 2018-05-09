package com.gzfns.obdpps.verticle;

import com.gzfns.obdpps.dataparser.VKT6DataHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiveDataVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(ReceiveDataVerticle.class);

    private static final String MESSAGE_TAIL = "#";

    @Override
    public void start() throws Exception {
        super.start();
        startTcpServer();
    }

    private void startTcpServer() {
        int port = config().getJsonObject("tcpServer").getInteger("port", 30013);
        Boolean logActivity = config().getJsonObject("tcpServer").getBoolean("log_activity", false);
        NetServerOptions options = new NetServerOptions().setPort(port).setLogActivity(logActivity);
        NetServer server = vertx.createNetServer(options);

        server.connectHandler(socket -> {
            logger.trace("socket arrived. writeHandlerID: " + socket.writeHandlerID());
            handleConnect(socket);
        }).listen(res -> {
            if (res.succeeded()) {
                logger.info("Tcp Server is now listening on actual port: " + server.actualPort());
            } else {
                logger.error("Tcp Server Failed to bind!", res.cause());
            }
        });
    }

    private void handleConnect(NetSocket socket) {
        logger.trace("writeHandlerID: {}", socket.writeHandlerID());
        VKT6DataHandler vkt6DataHandler = new VKT6DataHandler(vertx, socket.writeHandlerID());
        final RecordParser recordParser = RecordParser.newDelimited(Buffer.buffer(MESSAGE_TAIL), vkt6DataHandler);
        socket.handler(recordParser).exceptionHandler(exception -> {
            logger.error("socket exception : " + exception.getMessage());
            socket.close();
        }).closeHandler(exit -> {
            logger.trace("socket closed: " + socket.remoteAddress() + ", writeHandlerID: " + socket.writeHandlerID());
        });
    }

}
