package Hbase;

import com.zfylin.demo.bigdata.hbase.client.HBaseClientManager;

public class HBaseClientMangerTest {


	public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\tmp\\应用中心\\Hadoop\\hadoop-2.6.0");
		String quorum = "dev-121,dev-122,dev-123";
		int port = 2181;
		HBaseClientManager clientManager = new HBaseClientManager(quorum, port);

		System.out.println("conn => " + clientManager.getConn());

		clientManager.closeConn();

	}
}
