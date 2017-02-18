package hive;

import com.zfylin.demo.bigdata.hive.jdbc.HiveConnDbcpManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HiveConnDbcpManagerTest {


    public static void main(String[] args) throws Exception {
        HiveConnDbcpManager manager = HiveConnDbcpManager.getInstance();
        for (int i = 0; i < 5; i++) {
            test2(manager.getConnection());
        }
        manager.close();
    }

    private static void test2(Connection connection) throws Exception {
        System.setProperty("user.name", "root");
        System.out.println(">>>>> : " + connection);
        PreparedStatement st = connection.prepareStatement("DESCRIBE tbl_user_operation");
        ResultSet res = st.executeQuery();
        while (res.next()) {
            System.out.println(res.getString(1) + ":" + res.getString(2));
        }
        st.close();
        connection.close();
    }
}
