package com.zfylin.demo.bigdata.hbase.handler;

import com.zfylin.demo.bigdata.hbase.HBaseUtils;
import com.zfylin.demo.bigdata.hbase.client.HBaseClientManager;
import com.zfylin.demo.bigdata.hbase.client.HBaseConnPool;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseDMLHandler extends HBaseBaseHandler {

//	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDMLHandler.class);

	public HBaseDMLHandler(HBaseConnPool connPool) {
		super(connPool);
	}

	/**
	 * table put data
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family    列族
	 * @param qualifier 列
	 * @param data
	 * @throws Exception
	 */
	public void put(String tableName, String rowKey, String family, String qualifier, String data) throws Exception {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(data));
		table.put(put);

	}

	/**
	 * table put data
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifiers
	 * @param datas
	 * @throws Exception
	 */
	public void put(String tableName, String rowKey, String family, String[] qualifiers, String[] datas) throws Exception {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		for (int i = 0; i < qualifiers.length; i++) {
			if (null != datas && i < datas.length) {
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifiers[i]), Bytes.toBytes(datas[i]));
			} else {
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifiers[i]), null);
			}
		}
		table.put(put);

	}

	/**
	 * table put data[]
	 *
	 * @param tableName
	 * @param rowKeys
	 * @param family
	 * @param qualifier
	 * @param datas
	 * @throws Exception
	 */
	public void putList(String tableName, String[] rowKeys, String family, String qualifier, String[] datas) throws Exception {
		Table table = getTable(tableName);
		List<Put> putList = new ArrayList<Put>();
		for (int i = 0; i < rowKeys.length; i++) {
			Put put = new Put(Bytes.toBytes(rowKeys[i]));
			if (null != datas && i < datas.length) {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(datas[i]));
			} else {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), null);
			}
			putList.add(put);
		}
		table.put(putList);

	}

	/**
	 * get a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public Result get(String tableName, String rowKey) throws Exception {
		Table table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
        return table.get(get);
	}

	/**
	 * get a row column family by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @throws Exception
	 */
	public Result get(String tableName, String rowKey, String family) throws Exception {
		Table table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
        return table.get(get);
	}

	/**
	 * get a row column qualifier by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws Exception
	 */
	public Result get(String tableName, String rowKey, String family, String qualifier) throws Exception {
		Table table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return table.get(get);
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteRow(String tableName, String rowKey) throws Exception {
		Table table = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteFamily(String tableName, String rowKey, String family) throws Exception {
		Table table = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addFamily(Bytes.toBytes(family));
		table.delete(delete);
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteQualifier(String tableName, String rowKey, String family, String qualifier) throws Exception {
		Table table = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		table.delete(delete);
	}

	/**
	 * return all row from a table
	 *
	 * @param table
	 * @throws Exception
	 */
	public static ResultScanner scanAll(Table table) throws Exception {
		Scan s = new Scan();
        return table.getScanner(s);
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\tmp\\应用中心\\Hadoop\\hadoop-2.6.0");
		String quorum = "10.206.19.121,10.206.19.122,10.206.19.123";
		int port = 2181;
		HBaseConnPool connPool = new HBaseClientManager(quorum, port);
		HBaseDMLHandler handler = new HBaseDMLHandler(connPool);

		String tableName = "demo_test";
		handler.put(tableName, "key001", "cf", "name", "Michael");
		handler.put(tableName, "key001", "cf", "sex", "male");
		handler.put(tableName, "key001", "cf2", "blog", "micmiu.com");
		handler.put(tableName, "key001", "cf2", "github", "github.com/micmiu");

		handler.put(tableName, "key002", "cf", new String[]{"name", "sex"}, new String[]{"test", "female"});
		handler.putList(tableName, new String[]{"key010", "key011"}, "cf", "name", new String[]{"Michael010", "Michael011"});

		HBaseUtils.printResultInfo(handler.get(tableName, "key001"));
		HBaseUtils.printResultInfo(handler.get(tableName, "key001", "cf"));
		HBaseUtils.printResultInfo(handler.get(tableName, "key001", "cf2", "blog"));

		connPool.closeConn();
	}

}
