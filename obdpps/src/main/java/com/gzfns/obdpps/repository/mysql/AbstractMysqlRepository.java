package com.gzfns.obdpps.repository.mysql;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMysqlRepository {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMysqlRepository.class);

    private final static JsonObject mysqlConfig = Vertx.currentContext().config().getJsonObject("mysql");
    private final static String ip = mysqlConfig.getString("ip", "127.0.0.1");
    private final static String user = mysqlConfig.getString("user", "root");
    private final static String password = mysqlConfig.getString("password", "123456");
    private final static String dbName = mysqlConfig.getString("dbname", "obd");
    private final static int port = mysqlConfig.getInteger("port", 3306);
    private final static int poolSize = mysqlConfig.getInteger("max_pool_size", 80);
    private final static boolean useSSL = mysqlConfig.getBoolean("usessl", false);

    private final static String jdbcUrl = "jdbc:mysql://" + ip + ":" + port + "/" + dbName
                                        + "?useSSL=" + useSSL
                                        + "&serverTimezone=Asia/Shanghai"
//                                        + "&autoReconnect=true&failOverReadOnly=false&autoReconnectForPools=true"
                                        + "&useUnicode=true&characterEncoding=utf8";

    private final static JsonObject hikaricp_config = new JsonObject()
                                    .put("provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider")
                                    .put("jdbcUrl", jdbcUrl)
                                    .put("driverClassName", "com.mysql.cj.jdbc.Driver")
                                    .put("minimumIdle", 40)
                                    .put("maximumPoolSize", poolSize)
                                    .put("cachePrepStmts", true)
                                    .put("prepStmtCacheSize", 250)
                                    .put("prepStmtCacheSqlLimit", 2048)
                                    .put("useServerPrepStmts", true)
                                    .put("useLocalSessionState", true)
                                    .put("useLocalTransactionState", true)
                                    .put("rewriteBatchedStatements", true)
                                    .put("cacheResultSetMetadata", true)
                                    .put("cacheServerConfiguration", true)
                                    .put("elideSetAutoCommits", true)
                                    .put("maintainTimeStats", false)
                                    .put("idleTimeout", 60000)
                                    .put("connectionTimeout", 60000)
                                    .put("maxLifetime", 300000)
                                    .put("validationTimeout", 3000)
                                    .put("leakDetectionThreshold", 5000)
                                    .put("connectionTestQuery","/* ping */ SELECT 1")
                                    .put("username", user)
                                    .put("password", password);

    JDBCClient jdbcClient;

    AbstractMysqlRepository(Vertx vertx){
        jdbcClient = JDBCClient.createShared(vertx, hikaricp_config);
    }

    Handler<AsyncResult<SQLConnection>> connHandler(Future future, Handler<SQLConnection> handler) {
        return conn -> {
            if (conn.succeeded()) {
                final SQLConnection connection = conn.result();
                handler.handle(connection);
            } else {
                logger.error("Failed get mysql connection.", conn.cause());
                future.fail(conn.cause());
            }
        };
    }
}
