package com.seven.spark.hbase.rowkey.generator;

import com.seven.spark.hbase.rowkey.RowKeyGenerator;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件表RowKey
 *
 * @author seven
 */
public class FileRowKeyGenerator implements RowKeyGenerator<String> {
    private static final Logger LOG = LoggerFactory.getLogger(FileRowKeyGenerator.class);

    @Override
    public byte[] generate(String filename) {
        String rowKey = DigestUtils.md5Hex(filename + "" + System.currentTimeMillis());
        LOG.debug("RowKey: " + rowKey);

        String chars = "abcdef";
        char start = chars.charAt((int)(Math.random() * 6));
        return Bytes.toBytes(start+rowKey);
    }
}
