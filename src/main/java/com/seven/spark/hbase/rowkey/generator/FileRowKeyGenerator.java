package com.seven.spark.hbase.rowkey.generator;

import com.seven.spark.hbase.rowkey.RowKeyGenerator;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 *         __   __
 *         \/---\/
 *          ). .(
 *         ( (") )
 *          )   (
 *         /     \
 *        (       )``
 *       ( \ /-\ / )
 *        w'W   W'w
 *
 * author   seven
 * email    sevenstone@yeah.net
 * date     2018/5/10 下午1:40
 *
 * 文件表RowKey
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
