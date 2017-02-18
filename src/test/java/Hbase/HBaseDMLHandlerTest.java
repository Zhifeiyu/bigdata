package Hbase;

import com.zfylin.demo.bigdata.hbase.client.HBaseClientManager;
import com.zfylin.demo.bigdata.hbase.client.HBaseConnPool;
import com.zfylin.demo.bigdata.hbase.handler.HBaseDMLHandler;

public class HBaseDMLHandlerTest {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\tmp\\应用中心\\Hadoop\\hadoop-2.6.0");
        String quorum = "hadoop-master,hadoop-slave1,hadoop-slave2";
        int port = 2181;
        HBaseConnPool connPool = new HBaseClientManager(quorum, port);
        HBaseDMLHandler dmlHandler = new HBaseDMLHandler(connPool);
        String tableName = "nubia_channel_test:tbl_test";

        try {
            dmlHandler.put(tableName, "123", "hQualifier1","c1","test2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
