package com.zfylin.demo.bigdata.hbase.client;

import org.apache.hadoop.conf.Configuration;

public class HBaseConnPoolManager extends HBaseConnAbstractPool {

	public HBaseConnPoolManager() {
		super();
	}

	public HBaseConnPoolManager(Configuration config) {
		super(config);
	}

	public HBaseConnPoolManager(String quorum, int port, String znode) {
		getConfig().set("hbase.zookeeper.property.clientPort", port + "");
		getConfig().set("hbase.zookeeper.quorum", quorum);
		getConfig().set("zookeeper.znode.parent", znode);
	}
}
