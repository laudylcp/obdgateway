package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.HeartBeatEntity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;

public interface IHeartbeatService {

    Future<Boolean> batchSave(Vertx vertx, HeartBeatEntity heartBeatEntity, int batchSize, boolean startSaving);

    Future<Boolean> insert(HeartBeatEntity heartBeatEntity);

    Future<List<HeartBeatEntity>> getHeartbeatListBy(int hadOfflineAlarm);

    Future<Boolean> updateHadOfflineAlarm(HeartBeatEntity heartBeatEntity);
}
