package com.zfylin.demo.bigdata.hbase.handler;

import com.zfylin.demo.bigdata.hbase.HBaseUtils;
import com.zfylin.demo.bigdata.hbase.client.HBaseConnPool;
import com.zfylin.demo.bigdata.hbase.client.HBaseConnPoolManager;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseBaseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBaseHandler.class);

	private HBaseConnPool connPool;

	public HBaseBaseHandler(HBaseConnPool connPool) {
		this.connPool = connPool;
	}

	public HBaseBaseHandler() {
		this.connPool = new HBaseConnPoolManager();
	}

	public HBaseConnPool getConnPool() {
		return connPool;
	}

	public Table getTable(String tableName) throws Exception {
		return connPool.getConn().getTable(TableName.valueOf(tableName));
	}

	public Boolean tableExist(String tableName) {
		Admin admin = null;
		TableName tblName = TableName.valueOf(tableName);
		try {
			admin = connPool.getConn().getAdmin();
			return admin.tableExists(tblName);
		} catch (Exception ex) {
			LOGGER.error("exist htable err", ex);
		} finally {
			HBaseUtils.close(admin);
		}
		return false;
	}
}
