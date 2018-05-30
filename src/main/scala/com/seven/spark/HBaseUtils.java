package com.seven.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * author seven
 * time   2018-05-07
 * hbase工具类
 */
public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(HConstants.ZOOKEEPER_QUORUM,"vm-xaj-bigdata-da-d01,vm-xaj-bigdata-da-d02,vm-xaj-bigdata-da-d03");
        configuration.set(HConstants.HBASE_DIR,"hdfs://vm-xaj-bigdata-da-d01:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance() {
        if(null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 获取table
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {
        HTable table = null;

        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 写入数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param column
     * @param value
     */
    public void put(String tableName,String rowKey,String cf,String column,String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //HTable table = HBaseUtils.getInstance().getTable("course_clickcount_lt");
        //System.out.print(table.getName().getNameAsString());

        HBaseUtils.getInstance().put("course_clickcount_lt","20171111_88","info","click_count","2");
    }
}
