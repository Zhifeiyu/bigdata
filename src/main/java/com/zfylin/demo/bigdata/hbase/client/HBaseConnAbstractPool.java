package com.zfylin.demo.bigdata.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HBaseConnAbstractPool implements HBaseConnPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnAbstractPool.class);
    private Configuration config;
    private Connection conn;

    public HBaseConnAbstractPool() {
        this.config = HBaseConfiguration.create();
    }

    public HBaseConnAbstractPool(Configuration config) {
        this.config = config;
    }


    public Configuration getConfig() {
        return config;
    }

    public synchronized void closeConn() {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public synchronized Connection getConn() {
        if (null == conn) {
            try {
                this.conn = ConnectionFactory.createConnection(this.config);
            } catch (Exception ex) {
                LOGGER.error("create conn err:", ex);
            }
        }
        return conn;
    }
}
