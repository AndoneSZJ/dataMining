package com.seven.spark.hbase.rowkey.generator;

import com.seven.spark.hbase.rowkey.RowKeyGenerator;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * @author seven
 */
public class MD5RowKeyGenerator implements RowKeyGenerator<String> {

    private long currentId = 1;

    @Override
    public byte[] generate(String s) {
        try {
            return DigestUtils.md5Hex(currentId + "" + System.currentTimeMillis()).getBytes();
        } finally {
            currentId++;
        }
    }
}
