package Hbase;

import com.zfylin.demo.bigdata.hbase.handler.HBaseDDLHandler;
import com.zfylin.demo.bigdata.hbase.client.HBaseClientManager;
import com.zfylin.demo.bigdata.hbase.client.HBaseConnPool;

public class HBaseDDLHandlerTest {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\tmp\\应用中心\\Hadoop\\hadoop-2.6.0");
		String quorum = "10.206.19.80";
		int port = 2181;
		HBaseConnPool connPool = new HBaseClientManager(quorum, port);
		HBaseDDLHandler ddlHandler = new HBaseDDLHandler(connPool);

		String tableName = "nubia_channel_test";
		System.out.println("=============================== : delete");
		ddlHandler.deleteTable(tableName);

		String columnFamily = "cf";
		System.out.println("=============================== : create");
		ddlHandler.createTable(tableName, columnFamily, "cf2");

//		System.out.println("=============================== : desc");
//		HBaseUtils.printTableInfo(ddlHandler.getTable(tableName));
//		System.out.println("=============================== : alter");
//		Admin admin = ddlHandler.getConnPool().getConn().getAdmin();
//		TableName tblName = TableName.valueOf(tableName);
//		admin.disableTable(tblName);
//		Table table = ddlHandler.getTable(tableName);
//		HTableDescriptor tableDesc = admin.getTableDescriptor(table.getName());
//		tableDesc.removeFamily(Bytes.toBytes("cf2"));
//		HColumnDescriptor newhcd = new HColumnDescriptor("cf3");
//		newhcd.setMaxVersions(2);
//		newhcd.setKeepDeletedCells(KeepDeletedCells.TRUE);
//		tableDesc.addFamily(newhcd);
//
//        admin.modifyTable(tblName, tableDesc);
//		admin.enableTable(tblName);
//		admin.close();
//
//		System.out.println("=============================== : desc");
//		HBaseUtils.printTableInfo(ddlHandler.getTable(tableName));
//		System.out.println("=============================== : delete");
//		ddlHandler.deleteTable(tableName);

		connPool.closeConn();
	}
}
