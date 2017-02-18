package com.zfylin.demo.bigdata.hive.jdbc;

import java.sql.Connection;

/**
 * Hive 连接池
 * <p>
 * Created by zfylin on 2016/12/2.
 */
public interface HiveConnManager {

    void setup();

    void close();

    Connection getConnection();
}
