package com.gzfns.obdpps.repository.redis;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public abstract class AbstractRedisRepository {

    private static JsonObject redisConfig = Vertx.currentContext().config().getJsonObject("redis");

    private final static String ip = redisConfig.getString("ip", "127.0.0.1");
    private final static int port = redisConfig.getInteger("port", 6379);
    private final static String password = redisConfig.getString("password", "123456");
    private final static String encoding = redisConfig.getString("encoding", "UTF-8");
//  private final static boolean tcpKeepAlive = redisConfig.getBoolean("tcpkeepalive", true);
//  private final static boolean tcpNoDelay = redisConfig.getBoolean("tcpnodelay",true);

    private final static RedisOptions redisOptions = new RedisOptions().setHost(ip).setPort(port).setAuth(password)
                                                                        .setEncoding(encoding)
                                                                        .setSelect(0);

    public RedisClient redis;

    AbstractRedisRepository(Vertx vertx){
        redis = RedisClient.create(vertx, redisOptions);
    }
}
