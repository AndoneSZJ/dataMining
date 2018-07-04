package com.seven.spark.entity;

import java.util.Objects;

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
 * date     2018/5/29 上午11:07
 */
public class Other {
    //id，用来标识存储在es中的id，用来更新和删除数据
    private String id ;

    private String rowKey;//HBase行健

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Other other = (Other) o;
        return Objects.equals(id, other.id) &&
                Objects.equals(rowKey, other.rowKey);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, rowKey);
    }

    @Override
    public String toString() {
        return "Other{" +
                "id='" + id + '\'' +
                ", rowKey='" + rowKey + '\'' +
                '}';
    }
}
