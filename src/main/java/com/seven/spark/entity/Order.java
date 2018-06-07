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
 * date     2018/5/28 下午1:22
 */
public class Order extends Other {
    //订单实体类
    private String userId;//用户id
    private long playTime;//支付时间
    private String netId;//网点id
    private String pointId;//点位id
    private String rowKey;//HBase行健

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getPlayTime() {
        return playTime;
    }

    public void setPlayTime(long playTime) {
        this.playTime = playTime;
    }

    public String getNetId() {
        return netId;
    }

    public void setNetId(String netId) {
        this.netId = netId;
    }

    public String getPointId() {
        return pointId;
    }

    public void setPointId(String pointId) {
        this.pointId = pointId;
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
        Order order = (Order) o;
        return playTime == order.playTime &&
                Objects.equals(userId, order.userId) &&
                Objects.equals(netId, order.netId) &&
                Objects.equals(pointId, order.pointId) &&
                Objects.equals(rowKey, order.rowKey);
    }

    @Override
    public int hashCode() {

        return Objects.hash(userId, playTime, netId, pointId, rowKey);
    }


    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", playTime=" + playTime +
                ", netId='" + netId + '\'' +
                ", pointId='" + pointId + '\'' +
                ", rowKey='" + rowKey + '\'' +
                '}';
    }
}
