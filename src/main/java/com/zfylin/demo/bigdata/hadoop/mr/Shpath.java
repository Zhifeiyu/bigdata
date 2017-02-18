package com.zfylin.demo.bigdata.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class Shpath {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] line = value.toString().split("\t");
            List<String> l = new ArrayList<String>();

            for(String lin :line){
                l.add(lin);
            }

            List <String>startEnd = new ArrayList<String>();
            for(String s : l){
                String g = s.substring(0,1)+s.substring((s.length())-1);
                if(!startEnd.contains(g))
                {
                    startEnd.add(g);
                }
            }

            List<String> uniqueStringList = new ArrayList<String>();
            java.util.Map finalMap = new HashMap();
            for(String s1 : startEnd){

                for(String s : l) {
                    if(s.startsWith(s1.substring(0,1)) && (s.endsWith(s1.substring((s1.length())-1)))){
                        uniqueStringList.add(s);
                    }
                }
                String smallestKey = null;
                int minSize = Integer.MAX_VALUE;
                String smallest = null;
                for(String s2 : uniqueStringList){

                    if(s2.length() < minSize) {
                        minSize = s2.length();
                        smallest  = s2;
                        smallestKey  = s1;
                    }
                    finalMap.put(s1,smallest);

                }
                uniqueStringList.clear();
            }output.collect(new Text(),new Text(finalMap.values().toString()));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Shpath.class);
        conf.setJobName("shpath");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        String hdfs = "hdfs://hadoop-master:9000";
        args = new String[]{hdfs + "/user/root/input",
                hdfs + "/user/root/output/WordCount/" + new Date().getTime()};
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(conf, new Path(args[0]));
        org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
