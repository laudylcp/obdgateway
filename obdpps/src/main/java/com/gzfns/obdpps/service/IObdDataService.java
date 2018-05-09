package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.ObdDataEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public interface IObdDataService {

    Future<Boolean> batchSave(Vertx vertx, ObdDataEntity obdDataEntity, int batchSize, boolean startSaving);

}
