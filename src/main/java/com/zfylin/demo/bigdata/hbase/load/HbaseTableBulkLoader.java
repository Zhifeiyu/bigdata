package com.zfylin.demo.bigdata.hbase.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HbaseTableBulkLoader extends Configured implements Tool {

    // configuration parameters
    // hbase.table.name should contain the name of the table
    public static String HBASE_TABLE_NAME = "nubia_channel:tbl_active_check";
    // hbase.family.name should contain the name of the column family
    public static String HBASE_FAMILY_NAME = "id,imei,shipping_date,stockout_time,active_status,active_time,order_sn,express_sn,active_region_id,active_region_name,customer,active_region_flag";
    // hbase.column.names should contain a comma delimited list of strings
    // to be used as the column names (aka qualifiers) for the data inserts
    public static String HBASE_COLUMN_NAMES = "";
    // hbase.key.index should contain a zero based index of the column to be used
    // as the row key
    public static String HBASE_KEY_INDEX = "1";

    public static class HbaseTableLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private byte[][] fieldNameBytes;
        private byte[] family;

        private int keyFieldIndex;

        private long checkpoint = 1000;
        private long count = 0;

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            String[] fieldNames = config.getStrings(HBASE_COLUMN_NAMES);
            fieldNameBytes = new byte[fieldNames.length][];
            keyFieldIndex = (int) config.getLong(HBASE_KEY_INDEX, 0);
            for (int i = 0; i < fieldNames.length; ++i) {
                if (fieldNames[i].equals("-"))
                    fieldNameBytes[i] = null;
                else
                    fieldNameBytes[i] = Bytes.toBytes(fieldNames[i]);
            }
            family = Bytes.toBytes(config.get(HBASE_FAMILY_NAME));
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException {

            String[] fields = line.toString().split("\\t");
            try {
                byte[] rowkey = Bytes.toBytes(fields[keyFieldIndex]);

                // Create Put
                Put put = new Put(rowkey);

                for (int i = 0; i < fields.length && i < fieldNameBytes.length; ++i) {
                    // skip name/value if the name was given as a "-"
                    if (fieldNameBytes[i] == null)
                        continue;
                    // skip name/value if the value is empty
                    if (fields[i].length() < 1)
                        continue;
                    put.addColumn(family, fieldNameBytes[i], Bytes.toBytes(fields[i]));
                }

                // Uncomment below to disable WAL. This will improve performance but means
                // you will experience data loss in the case of a RegionServer crash.
                put.setWriteToWAL(false);

                try {
                    context.write(new ImmutableBytesWritable(rowkey), put);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Set status every checkpoint lines
                if (++count % checkpoint == 0) {
                    context.setStatus("Emitting Put: " + count + " - " + Bytes.toString(rowkey));
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                // TODO increment a counter or something
            }
        }


    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        // Set job class and job name
        job.setJarByClass(getClass());
        job.setJobName("HbaseTableBulkLoader");

        // Set mapper class and reducer class
        job.setMapperClass(HbaseTableLoadMapper.class);
        job.setNumReduceTasks(0);

        // Hbase specific setup
        TableMapReduceUtil.initTableReducerJob(HBASE_TABLE_NAME, null, job);

        // Handle input path
        List<String> other_args = new ArrayList<>();
        for (int i = 0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));

        // Submit job to server and wait for completion
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\tmp\\应用中心\\Hadoop\\hadoop-2.6.0");

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave1,hadoop-slave2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        int res = ToolRunner.run(config, new HbaseTableBulkLoader(), new String[]{"/data/hive/warehouse/nubia_channel.db/tbl_active_check"});
        System.exit(res);
    }
}
