package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.DeviceAlarmEntity;
import io.vertx.core.Future;

public interface IAlarmService {

    Future<Boolean> insert(DeviceAlarmEntity deviceAlarmEntity);

    Future<Boolean> updateBy(DeviceAlarmEntity where);

}
