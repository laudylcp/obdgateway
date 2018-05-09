package com.gzfns.obdpps.senddown;


import io.vertx.core.Future;

import java.util.List;

public interface IDownSender<T> {

    Future<Boolean> senddown(List<T> commandArray, String writeHandlerID);

    Future<Boolean> senddown(T command, String writeHandlerID);

}
