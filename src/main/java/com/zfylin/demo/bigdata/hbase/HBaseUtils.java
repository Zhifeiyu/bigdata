package com.zfylin.demo.bigdata.hbase;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Hbase Table common handler method
 */
public class HBaseUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);
    private static final String DEAFULT_ENCODE = "UTF-8";

    public static void close(Admin admin) {
        try {
            if (null != admin) {
                admin.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * put a cell data into a row identified by rowKey,columnFamily,identifier
     *
     * @param table
     * @param rowKey
     * @param family
     * @param qualifier
     * @param data
     * @throws Exception
     */
    public static void put(Table table, String rowKey, String family, String qualifier, String data) throws Exception {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(data));
        table.put(put);
    }

    /**
     * get a row identified by rowkey
     *
     * @param table
     * @param rowKey
     * @throws Exception
     */
    public static Result get(Table table, String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        return table.get(get);
    }

    /**
     * delete a row by rowkey
     *
     * @param table
     * @param rowKey
     * @throws Exception
     */
    public static void deleteRow(Table table, String rowKey) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        LOGGER.info(">>>> HBase Delete {} row with key = {} ", table.getName(), rowKey);
    }

    /**
     * delete a row column family by rowkey
     *
     * @param table
     * @param rowKey
     * @throws Exception
     */
    public static void deleteFamily(Table table, String rowKey, String family) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(family));
        table.delete(delete);
        LOGGER.info(">>>> HBase Delete {} data with key = {}, columnFamily = {}.", table.getName(), rowKey, family);
    }

    /**
     * delete a row family:qualifier data by rowkey
     *
     * @param table
     * @param rowKey
     * @throws Exception
     */
    public static void deleteQualifier(Table table, String rowKey, String family, String qualifier) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        table.delete(delete);
        LOGGER.info(">>>> HBase Delete {} data with key = {}, columnFamily = {}, qualifier = {}.", table.getName(), rowKey, family, qualifier);
    }

    /**
     * return all row from a table
     *
     * @param table
     * @throws Exception
     */
    public static ResultScanner scanAll(Table table) throws Exception {
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        return rs;
    }


    /**
     * return all match row from a table by scan filter
     *
     * @param table
     * @throws Exception
     */
    public static ResultScanner scan(Table table, Scan s) throws Exception {
        ResultScanner rs = table.getScanner(s);
        return rs;
    }

    /**
     * print table info
     *
     * @param table
     */
    public static void printTableInfo(Table table) {
        try {
            HTableDescriptor desc = table.getTableDescriptor();
            LOGGER.info(">>>> Print Table {} Desc", table.getName().getName());
            for (HColumnDescriptor colDesc : desc.getColumnFamilies()) {
                LOGGER.info(">>>> family column: {}", colDesc.getNameAsString());

            }
        } catch (Exception ex) {
            LOGGER.error("printTable info Error:", ex);
        }
    }

    /**
     * print info for Result
     *
     * @param r
     */
    public static void printResultInfo(Result r) throws UnsupportedEncodingException {
        System.out.print(">>>> cell rowkey= [" + new String(r.getRow(), DEAFULT_ENCODE) + "]");
        for (Cell cell : r.rawCells()) {
            System.out.print(">>>> cell rowkey= " + new String(CellUtil.cloneRow(cell), DEAFULT_ENCODE));
            System.out.print(",family= " + new String(CellUtil.cloneFamily(cell), DEAFULT_ENCODE) + ":" +
                    new String(CellUtil.cloneQualifier(cell), DEAFULT_ENCODE));
            System.out.println(", value= [" + new String(CellUtil.cloneValue(cell), DEAFULT_ENCODE) + "]");
        }
    }
}
