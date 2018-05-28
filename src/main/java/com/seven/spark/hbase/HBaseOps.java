package com.seven.spark.hbase;

import com.google.common.collect.Lists;
import com.seven.spark.common.CommonConst;
import com.seven.spark.common.Utils;
import com.seven.spark.hbase.pool.HBasePool;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * HBase常用操作
 *
 * @author seven
 */
public class HBaseOps {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseOps.class);

    public static void closeAdmin(Admin admin) {
        Utils.close(admin);
    }

    /**
     * 关闭表
     */
    public static void closeTable(Table table) {
        Utils.close(table);
    }

    /**
     * 创建表,如果未指定列簇,默认为cf
     *
     * @param tableName 表名
     * @throws Exception
     */
    public static void createTable(String tableName)
            throws Exception {
        createTable(tableName, CommonConst.DEFAULT_HBASE_COLUMN_FAMILY);
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param families  列簇
     * @throws Exception
     */
    public static void createTable(String tableName, String... families)
            throws Exception {
        createTable(tableName, families, null);
    }

    public static void createTable(String tableName, String family, byte[][] splitKeys) throws Exception {
        createTable(tableName, new String[]{family}, splitKeys);
    }

    public static void createTable(String tableName, String[] families, byte[][] splitKeys) throws Exception {
        Admin admin = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            admin = conn.getAdmin();
            TableName name = TableName.valueOf(tableName);
            if (admin.tableExists(name)) {
//                admin.disableTable(tableName);
//                admin.deleteTable(tableName);
                LOG.warn("table [{}] is exists", tableName);
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(name);
                // 设置split策略
//                tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
//                tableDesc.setValue("MEMSTORE_FLUSHSIZE", "5242880"); // 5M
                if (null != families) {
                    for (String family : families) {
                        HColumnDescriptor columnDesc = new HColumnDescriptor(family);
//                    columnDesc.setMinVersions(1);
//                    columnDesc.setBlocksize(524288); // 512k,默认64k
                        tableDesc.addFamily(columnDesc);
                    }
                }
                admin.createTable(tableDesc, splitKeys);
            }
        } finally {
            closeAdmin(admin);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    public static Table getTable(String tableName) throws Exception {
        Connection conn = HBasePool.getInstance().borrowObject();
        Table table = conn.getTable(TableName.valueOf(tableName));
        HBasePool.getInstance().returnObject(conn);
        return table;
    }

    /**
     * 获取所有的表
     *
     * @return 表集合
     * @throws Exception
     */
    public static List<String> showTables()
            throws Exception {
        Admin admin = null;
        Connection conn = null;
        try {
            List<String> tables = Lists.newArrayList();
            conn = HBasePool.getInstance().borrowObject();
            admin = conn.getAdmin();
            HTableDescriptor descriptors[] = admin.listTables();
            for (HTableDescriptor descriptor : descriptors) {
                tables.add(descriptor.getNameAsString());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("tables: {}", tables);
            }
            return tables;
        } finally {
            closeAdmin(admin);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws Exception
     */
    public static void deleteTable(String tableName)
            throws Exception {
        Admin admin = null;
        Connection conn = null;
        try {
            TableName name = TableName.valueOf(tableName);
            conn = HBasePool.getInstance().borrowObject();
            admin = conn.getAdmin();
            if (admin.tableExists(name)) {
                admin.disableTable(name); // 删除表之前,先disable
                admin.deleteTable(name);
            } else {
                LOG.warn("table [{}] not exists", tableName);
            }
        } finally {
            closeAdmin(admin);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 插入行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 修饰符
     * @param value     内容
     * @throws Exception
     */
    public static void put(
            String tableName,
            String rowKey,
            String family,
            String qualifier,
            String value
    ) throws Exception {
        put(
                tableName,
                rowKey,
                family,
                qualifier,
                Bytes.toBytes(value)
        );
    }

    /**
     * 插入行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 修饰符
     * @param value     内容
     * @throws Exception
     */
    public static void put(
            String tableName,
            String rowKey,
            String family,
            String qualifier,
            byte[] value
    ) throws Exception {
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
            table.put(put);
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 插入行
     *
     * @param tableName 表名
     * @param put       put
     * @throws Exception
     */
    public static void put(
            String tableName,
            Put put
    ) throws Exception {
        long start = System.currentTimeMillis();
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            table.put(put);
            LOG.debug("put to hbase took {} ms", System.currentTimeMillis() - start);
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 批量插入行
     *
     * @param tableName 表名
     * @param puts      put列表
     * @throws Exception
     */
    public static void put(
            String tableName,
            List<Put> puts
    ) throws Exception {
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            table.put(puts);
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 删除行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @throws Exception
     */
    public static void deleteRow(String tableName, String rowKey)
            throws Exception {
        deleteRow(tableName, rowKey, null, null);
    }


    /**
     * 删除行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 标识
     * @throws Exception
     */
    public static void deleteRow(String tableName, String rowKey, String family, String qualifier)
            throws Exception {
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            // 删除指定列簇
            // delete.addFamily(Bytes.toBytes(colFamily));
            // 删除指定列
            // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            if (!StringUtils.isEmpty(family) && StringUtils.isEmpty(qualifier)) {
                delete.addFamily(Bytes.toBytes(family));
            }
            if (!StringUtils.isEmpty(family) && !StringUtils.isEmpty(qualifier)) {
                delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            }
            table.delete(delete);
            // 批量删除
//        List<Delete> deletes = Lists.newArrayList();
//        deletes.add(delete);
//        table.delete(deletes);
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 获取行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @throws Exception
     */
    public static byte[] getRow(String tableName, String rowKey)
            throws Exception {
        return getRow(tableName, rowKey, null);
    }

    /**
     * 获取行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @throws Exception
     */
    public static byte[] getRow(String tableName, String rowKey, String family)
            throws Exception {
        return getRow(tableName, rowKey, family, null);
    }

    /**
     * 获取行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 标识
     * @throws Exception
     */
    public static byte[] getRow(String tableName, String rowKey, String family, String qualifier)
            throws Exception {
        long start = System.currentTimeMillis();
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!StringUtils.isEmpty(family) && StringUtils.isEmpty(qualifier)) {
                get.addFamily(Bytes.toBytes(family));
            }
            if (!StringUtils.isEmpty(family) && !StringUtils.isEmpty(qualifier)) {
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            }
            Result result = table.get(get);
            if (result.isEmpty()) {
                LOG.warn("result is empty");
            } else {
                if (LOG.isDebugEnabled()) {
                    showCell(result); // 显示cell信息
                }
            }
            byte[] data = new byte[0];
            for (Cell cell : result.rawCells()) {
                data = CellUtil.cloneValue(cell);
            }
            LOG.debug("get took {} ms", System.currentTimeMillis() - start);
            return data;
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }


    /**
     * 获取多行
     *
     * @param tableName 表名
     * @param rowKeys    行键
     * @throws Exception
     */
    public static List<byte[]> getRows(String tableName, List<String> rowKeys)
            throws Exception {
        return getRows(tableName, rowKeys, null);
    }

    /**
     * 获取多行
     *
     * @param tableName 表名
     * @param rowKeys    行键
     * @param family    列簇
     * @throws Exception
     */
    public static List<byte[]> getRows(String tableName, List<String> rowKeys, String family)
            throws Exception {
        return getRows(tableName, rowKeys, family, null);
    }

    /**
     * 获取多行
     *
     * @param tableName 表名
     * @param rowKeys    行键
     * @param family    列簇
     * @param qualifier 标识
     * @throws Exception
     */
    public static List<byte[]> getRows(String tableName, List<String> rowKeys, String family, String qualifier)
            throws Exception {
        long start = System.currentTimeMillis();
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            List<Get> gets = new ArrayList<Get>();
            for(String rowKey : rowKeys){
                Get get = new Get(Bytes.toBytes(rowKey));
                if (!StringUtils.isEmpty(family) && StringUtils.isEmpty(qualifier)) {
                    get.addFamily(Bytes.toBytes(family));
                }
                if (!StringUtils.isEmpty(family) && !StringUtils.isEmpty(qualifier)) {
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                }
                gets.add(get);
            }
            Result[] results = table.get(gets);
            if (results.length == 0) {
                LOG.warn("result is empty");
            } else {
                if (LOG.isDebugEnabled()) {
                    showCell(results[0]); // 显示cell信息
                }
            }
            List<byte[]> list = new ArrayList<byte[]>();
            for(Result result : results){
                byte[] data = new byte[0];
                for (Cell cell : result.rawCells()) {
                    data = CellUtil.cloneValue(cell);
                }
                list.add(data);
            }

            LOG.debug("get took {} ms", System.currentTimeMillis() - start);
            return list;
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }





    /**
     * 判断结果是否为空
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @return 结果为空返回true, 否则返回false
     * @throws Exception
     */
    public static boolean isEmpty(String tableName, String rowKey)
            throws Exception {
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            return result.isEmpty();
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    private static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("row key: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("timestamp: " + cell.getTimestamp());
            System.out.println("column family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("qualifier: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value length: " + CellUtil.cloneValue(cell).length);
//            System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 扫描表
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @throws Exception
     */
    public static void scan(String tableName, String startRow, String stopRow)
            throws Exception {
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if (!StringUtils.isEmpty(startRow) && !StringUtils.isEmpty(stopRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
                scan.setStopRow(Bytes.toBytes(stopRow));
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                showCell(result);
            }
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 扫描表
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @throws Exception
     */
    public static void scanWithFilter(String tableName, String startRow, String stopRow)
            throws Exception {
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            // FilterList代表一个过滤器列表
            // FilterList.Operator.MUST_PASS_ALL=>and
            // FilterList.Operator.MUST_PASS_ONE=>or
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

            // 提取RowKey以01结尾的行
            Filter regexFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*01$"));
            filterList.addFilter(regexFilter);

            // 提取RowKey以包含201610的行
            Filter substringFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("201610"));
            filterList.addFilter(substringFilter);

            // 提取RowKey以123开头的行
            Filter prefixFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator("123".getBytes()));
            filterList.addFilter(prefixFilter);

            scan.setFilter(filterList);
            if (!StringUtils.isEmpty(startRow) && !StringUtils.isEmpty(stopRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
                scan.setStopRow(Bytes.toBytes(stopRow));
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                showCell(result);
            }
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    /**
     * 扫描部分数据
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param topNum
     * @return
     */
    public static List<Result> scanWithTopFilter(String tableName, String startRow, String stopRow, int topNum)
            throws Exception{
        HTable table = null;
        Connection conn = null;
        try {
            conn = HBasePool.getInstance().borrowObject();
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setCaching(1);
            // FilterList代表一个过滤器列表
            // FilterList.Operator.MUST_PASS_ALL=>and
            // FilterList.Operator.MUST_PASS_ONE=>or
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            PageFilter pageFilter = new PageFilter(topNum);
            filterList.addFilter(pageFilter);
            scan.setFilter(filterList);
            if (!StringUtils.isEmpty(startRow) && !StringUtils.isEmpty(stopRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
                scan.setStopRow(Bytes.toBytes(stopRow));
            }
            ResultScanner scanner = table.getScanner(scan);
            List<Result> results = new ArrayList<Result>();
            for (Result result : scanner) {
                results.add(result);
            }
            return results;
        } finally {
            closeTable(table);
            HBasePool.getInstance().returnObject(conn);
        }
    }

    public static void main(String[] args) {
        try {
//            createTable("seven",new String[]{"family","qualifier"});
//            deleteTable("orderDataTest");
            List<String> stringList = new ArrayList<String>();
            stringList.add("aaa5406793327251");
            stringList.add("aaa1396793327251");
            stringList.add("aaa3495793327251");
            List<byte []> list = getRows("seven",stringList,"family","qualifier");
            for(byte[] b : list){
                System.out.println(new String(b));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
