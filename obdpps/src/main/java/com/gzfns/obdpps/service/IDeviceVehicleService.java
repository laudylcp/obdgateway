package com.gzfns.obdpps.service;

import com.gzfns.obdpps.entity.DeviceVehicleEntity;
import io.vertx.core.Future;

public interface IDeviceVehicleService {

    Future<Boolean> insert(DeviceVehicleEntity deviceVehicleEntity);
}
