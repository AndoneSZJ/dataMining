package com.seven.spark.hbase.rowkey;

import java.io.Serializable;

/**
 * RowKey生成器,RowKey设计三原则
 * <li>长度</li>
 * <li>散列</li>
 * <li>唯一</li>
 *
 * @author seven
 */
public interface RowKeyGenerator<T> extends Serializable {

    /**
     * 生成RowKey
     *
     * @return RowKey
     */
    byte[] generate(T t);
}
