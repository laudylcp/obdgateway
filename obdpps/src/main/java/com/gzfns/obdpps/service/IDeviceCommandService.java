package com.gzfns.obdpps.service;

import io.vertx.core.Future;

import java.util.List;


public interface IDeviceCommandService<T> {

    Future<List<T>> getRedisList(T entity);

    Future<Boolean> saveList(List<T> entityList);

    Future<Boolean> updateSendStatus(T entity);

    Future<Boolean> updateReplyStatus(T entity);

}
