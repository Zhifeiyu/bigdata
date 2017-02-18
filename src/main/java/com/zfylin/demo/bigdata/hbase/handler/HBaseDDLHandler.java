package com.zfylin.demo.bigdata.hbase.handler;

import com.zfylin.demo.bigdata.hbase.client.HBaseConnPool;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HTable DDL handler
 */
public class HBaseDDLHandler extends HBaseBaseHandler {


    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBaseHandler.class);

    public HBaseDDLHandler(HBaseConnPool connPool) {
        super(connPool);
    }

    /**
     * create table by familyNames
     *
     * @param tableName
     * @param familyNames
     * @return
     * @throws Exception
     */
    public Boolean createTable(String tableName, String... familyNames) throws Exception {
        if (tableExist(tableName)) {
            LOGGER.warn(">>>> Table {} exists!", tableName);
            return false;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDesc.addFamily(new HColumnDescriptor(familyName));
        }

        return this.createTable(tableDesc);

    }

    /**
     * create table by HTableDescriptor
     *
     * @param tableDesc
     * @return
     * @throws Exception
     */
    public Boolean createTable(HTableDescriptor tableDesc) throws Exception {
        Admin admin = getConnPool().getConn().getAdmin();
        admin.createTable(tableDesc);
        admin.close();
        LOGGER.info(">>>> Table {} create success!", tableDesc.getTableName().getNameAsString());
        return true;
    }

    /**
     * list all table name
     *
     * @return
     * @throws Exception
     */
    public List<String> listTables() throws Exception {

        Admin admin = getConnPool().getConn().getAdmin();
        List<String> tableNameList = new ArrayList<String>();
        for (TableName tableName : admin.listTableNames()) {
            tableNameList.add(tableName.getNameAsString());
        }
        return tableNameList;
    }

    /**
     * @param tableName
     * @return
     */
    public boolean deleteTable(String tableName) throws IOException {
        Admin admin = getConnPool().getConn().getAdmin();
        TableName tblName = TableName.valueOf(tableName);
        if (admin.tableExists(tblName)) {
            try {
                if (admin.isTableEnabled(tblName)) {
                    admin.disableTable(tblName);
                }
                admin.deleteTable(tblName);
                LOGGER.info(">>>> Table {} delete success!", tblName.getName());
            } catch (Exception ex) {
                LOGGER.error("delete table error:", ex);
                return false;
            }
        } else {
            LOGGER.warn(">>>> Table {} delete but not exist.", tblName.getName());
        }
        admin.close();
        return true;
    }
}
