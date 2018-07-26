package com.seven.spark.entity;

import java.util.Objects;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    zjshi01@mail.nfsq.com.cn
 * date     2018/7/26 上午11:14
 */
public class OrderTest {
    private String pk;
    private String id;
    private String time;
    private Double money;
    private String account;
    private String other;

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    @Override
    public String toString() {
        return "OrderTest{" +
                "pk='" + pk + '\'' +
                ", id='" + id + '\'' +
                ", time='" + time + '\'' +
                ", money=" + money +
                ", account='" + account + '\'' +
                ", other='" + other + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderTest orderTest = (OrderTest) o;
        return Objects.equals(pk, orderTest.pk) &&
                Objects.equals(id, orderTest.id) &&
                Objects.equals(time, orderTest.time) &&
                Objects.equals(money, orderTest.money) &&
                Objects.equals(account, orderTest.account) &&
                Objects.equals(other, orderTest.other);
    }

    @Override
    public int hashCode() {

        return Objects.hash(pk, id, time, money, account, other);
    }
}
