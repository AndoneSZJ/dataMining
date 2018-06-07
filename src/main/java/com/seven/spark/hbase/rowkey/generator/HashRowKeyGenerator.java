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
 * date     2018/5/10 下午1:50
 *
 * Hash RowKey生成器
 */
public class HashRowKeyGenerator implements RowKeyGenerator<String> {
    private static final Logger LOG = LoggerFactory.getLogger(HashRowKeyGenerator.class);

//    private long currentId = 1;
//    private long currentTime = System.currentTimeMillis();

//    private Random random = new Random();

//    public byte[] generate(String s) {
//        try {
//            currentTime += random.nextInt(1000);
//
//            byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);
//            byte[] lowU = Bytes.copy(Bytes.toBytes(currentId), 4, 4);
//
//
//            String id = String.format("%012d", currentId);
//            return Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.add(lowU, lowT)).substring(0, 4) + id);
//        } finally {
//            currentId++;
//        }
//    }
    @Override
    public byte[] generate(String filename) {//时间搓反转，加上三个随机数
        String rowKey = String.valueOf(System.currentTimeMillis());
        rowKey = new StringBuilder(rowKey).reverse().toString();
        String chars = "abcdefghijklnmopqrstuvwx";
        char start = chars.charAt((int)(Math.random() * 24));
        char middle = chars.charAt((int)(Math.random() * 24));
        char end = chars.charAt((int)(Math.random() * 24));
        rowKey = start+""+middle+""+end+rowKey;
        LOG.debug("RowKey: " + rowKey);
        return Bytes.toBytes(rowKey);
    }

}
