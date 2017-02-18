package com.zfylin.demo.bigdata.hive.jdbc;


import com.zfylin.demo.bigdata.utils.Properties;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * DBCP 实现 Hive 连接池
 * <p>
 * Created by zfylin on 2016/12/2.
 */
public class HiveConnDbcpManager implements HiveConnManager {

    private static final String CONF_FILE_NAME = "jdbc.properties";

    private static HiveConnDbcpManager instance; // 唯一实例
    private BasicDataSource bds = null;

    public static synchronized HiveConnDbcpManager getInstance() {
        if (instance == null) {
            instance = new HiveConnDbcpManager();
        }
        return instance;
    }

    /**
     * 建构函数私有以防止其它对象创建本类实例
     */
    private HiveConnDbcpManager() {
        setup();
    }


    public void setup() {
        if (bds != null) {
            close();
        }
        Properties properties = new Properties(CONF_FILE_NAME);

        bds = new BasicDataSource();
        bds.setDriverClassName(properties.getProperty("hive.jdbc.driver", "org.apache.hive.jdbc.HiveDriver"));
        bds.setUrl(properties.getProperty("hive.jdbc.url"));
        bds.setUsername(properties.getProperty("hive.jdbc.user", ""));
        bds.setPassword(properties.getProperty("hive.jdbc.password", ""));
        int maxActive = 0;
        try {
            maxActive = properties.getPropertyInteger("hive.jdbc.maxActive");
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        if (maxActive < -1)
            maxActive = -1;
        bds.setMaxActive(maxActive);
        bds.setMaxIdle(1);
        bds.setInitialSize(1);
    }

    public void close() {
        try {
            bds.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close(Connection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public synchronized Connection getConnection() {
        try {
            return bds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


}
