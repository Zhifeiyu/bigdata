package com.zfylin.demo.bigdata.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public interface HBaseConnPool {

    Configuration getConfig();

    Connection getConn();

    void closeConn();
}
