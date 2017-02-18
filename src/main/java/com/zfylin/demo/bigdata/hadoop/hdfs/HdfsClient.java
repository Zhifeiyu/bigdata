package com.zfylin.demo.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * HDFS java客户端的基本操作
 */
public class HdfsClient {

    private static volatile Configuration conf = null;
    private static FileSystem fs = null;

    public HdfsClient() {
        setConf();
    }

    private static synchronized void setConf() {
        conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        try {
            fs = FileSystem.get(conf);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void printInfo() {
        System.out.println(">>>> fs uri    = " + fs.getUri());
        System.out.println(">>>> fs scheme = " + fs.getScheme());
        Path home = fs.getHomeDirectory();
        System.out.println(">>>> home path = " + home.toString());
        listDataNodeInfo();

    }

    public boolean checkFileExist(String fileName) throws Exception {
        Path path = new Path(fileName);
        return fs.exists(path);
    }

    /**
     * 读取hdfs指定目录下文件列表
     *
     * @param path
     * @param recursion
     * @throws Exception
     */
    public List<String> listFile(Path path, boolean recursion) throws Exception {
        List<String> files = new ArrayList<>();

        FileStatus[] fileStatusList = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatusList) {
            if (fileStatus.isDirectory()) {
                if (recursion) {
                    files.addAll(listFile(fileStatus.getPath(), recursion));
                }
            } else {
                files.add(fileStatus.getPath().getName());
            }
        }

        return files;
    }


    /**
     * 创建目录
     *
     * @param path
     * @throws Exception
     */
    public boolean mkdir(String path) throws Exception {
        Path pathTmp = new Path(path);
        if (fs.exists(pathTmp)) {
            return false;
        }
        return fs.mkdirs(pathTmp);
    }

    /**
     * 创建hdfs文件
     */
    public void createFile(String filename, String data) throws Exception {
        FSDataOutputStream os = null;
        BufferedWriter bw = null;
        try {
            Path filePath = new Path(filename);
            os = fs.create(filePath, true);
            bw = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));
            bw.write(data);
            bw.newLine();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != bw) {
                bw.close();
            }
            if (null != os) {
                os.close();
            }
        }

    }

    /**
     * 创建一个新的空文件
     */
    public boolean createFile(String filename) throws Exception {
        return fs.createNewFile(new Path(filename));
    }

    /**
     * 创建hdfs文件
     */
    public FSDataOutputStream createFile2(String filename) throws Exception {
        Path filePath = new Path(filename);
        return fs.create(filePath, true);
    }

    /**
     * 读取hdfs中的文件内容
     */
    public List<String> readFile(String path) throws Exception {
        FSDataInputStream is = null;
        BufferedReader br = null;
        List<String> data = new ArrayList<>();
        try {
            Path filePath = new Path(path);
            is = fs.open(filePath);
            br = new BufferedReader(new InputStreamReader(is, "utf-8"));
            String line;
            while ((line = br.readLine()) != null) {
//                System.out.println(">>>> line : " + line);
                data.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                br.close();
            }
            if (null != is) {
                is.close();
            }
        }

        return data;
    }

    /**
     * 取得文件块所在的位置..
     */
    public void getFileBlockLocation(String pathuri) {
        try {
            Path filePath = new Path(pathuri);
            FileStatus fileStatus = fs.getFileStatus(filePath);
            if (fileStatus.isDirectory()) {
                System.out.println("**** getFileBlockLocations only for file");
                return;
            }
            System.out.println(">>>> file block location:");
            BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            for (BlockLocation currentLocation : blkLocations) {
                String[] hosts = currentLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(">>>> host: " + host);
                }
            }

            //取得最后修改时间
            long modifyTime = fileStatus.getModificationTime();
            Date d = new Date(modifyTime);
            System.out.println(">>>> ModificationTime = " + d);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 列出所有DataNode的名字信息
     */
    public void listDataNodeInfo() {
        try {
            DistributedFileSystem hdfs = (DistributedFileSystem) fs;
            DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
            String[] names = new String[dataNodeStats.length];
            System.out.println(">>>> List of all the datanode in the HDFS cluster:");

            for (int i = 0; i < names.length; i++) {
                names[i] = dataNodeStats[i].getHostName();
                System.out.println(">>>> datanode : " + names[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 读取本地文件上传到HDFS
     *
     * @param localFileStr
     * @param dstFileStr
     */
    public void putFileToHDFS(String localFileStr, String dstFileStr) {
        putFileToHDFS(true, localFileStr, dstFileStr);
    }

    /**
     * 手工IO实现把本地文件上传到HDFS
     *
     * @param override
     * @param localFileStr
     * @param dstFileStr
     */
    public void putFileToHDFS(Boolean override, String localFileStr, String dstFileStr) {
        FileInputStream is = null;
        BufferedReader br = null;
        FSDataOutputStream os = null;
        BufferedWriter bw = null;
        try {
            File localFile = new File(localFileStr);
            is = new FileInputStream(localFile);
            br = new BufferedReader(new InputStreamReader(is, "utf-8"));
            Path dstTmpPath = new Path(dstFileStr);
            Path dstPath = dstTmpPath;
            if (fs.exists(dstTmpPath)) {
                FileStatus fileStatus = fs.getFileStatus(dstTmpPath);
                if (fileStatus.isDirectory()) {
                    dstPath = new Path(dstTmpPath.toString() + "/" + localFile.getName());
                } else if (!override) {
                    System.out.println("**** dst file is exist, can't override.");
                    return;
                }
            }
            os = fs.create(dstPath, true);
            bw = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));

            String line;
            while ((line = br.readLine()) != null) {
                bw.write(line);
                bw.newLine();
            }
            System.out.println(">>>> put local " + localFile.getName() + " to hdfs " + dstPath.toString() + " success");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            IOUtils.closeStream(bw);
            IOUtils.closeStream(os);
            IOUtils.closeStream(br);
            IOUtils.closeStream(is);

        }
    }

    /**
     * 本地文件上传hdfs
     *
     * @param override
     * @param localFileStr
     * @param dstFileStr
     */
    public void copyFromLocalFile(Boolean override, String localFileStr, String dstFileStr) {
        try {
            fs.copyFromLocalFile(false, override, new Path(localFileStr), new Path(dstFileStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 本地文件上传hdfs
     *
     * @param localFileStr
     * @param dstFileStr
     */
    public void copyFromLocalFile(String localFileStr, String dstFileStr) {
        this.copyFromLocalFile(true, localFileStr, dstFileStr);
    }

    /**
     * 复制hdfs文件到本地
     *
     * @param delSrc
     * @param localFileStr
     * @param dstFileStr
     */
    public void copyToLocalFile(Boolean delSrc, String localFileStr, String dstFileStr) {
        try {
            fs.copyToLocalFile(delSrc, new Path(localFileStr), new Path(dstFileStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     *
     * 复制hdfs文件到本地
     *
     * @param localFileStr
     * @param dstFileStr
     */
    public void copyToLocalFile(String localFileStr, String dstFileStr) {
        copyToLocalFile(false, localFileStr, dstFileStr);
    }

}


