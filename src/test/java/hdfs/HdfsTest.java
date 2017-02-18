package hdfs;

import com.zfylin.demo.bigdata.hadoop.hdfs.HdfsClient;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author zfylin
 * @date 14:24 2017/2/9
 */
public class HdfsTest {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        HdfsClient client = new HdfsClient();
        String filePath = "hdfs:///data/hive/warehouse/channel_test.db/tbl_student/";
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-hhmmss");
        String fileName = filePath + df.format(new Date());

        try {
//


            System.out.println("-------------------------------------------------");
            List<String> data = client.readFile(filePath + "2017-02-09-055746");
            data.forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
