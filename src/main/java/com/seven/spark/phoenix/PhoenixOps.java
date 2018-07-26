package com.seven.spark.phoenix;

import com.seven.spark.entity.OrderTest;
import com.seven.spark.phoenix.pool.PhoenixPool;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    zjshi01@mail.nfsq.com.cn
 * date     2018/7/24 上午11:19
 */
public class PhoenixOps {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixOps.class);

    public static void main(String[] args) {
        try {
            List<OrderTest> orderTests = getOrderTest("ordertest");
            for(OrderTest o : orderTests){
                System.out.println(o.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<OrderTest> getOrderTest(String tableName) throws Exception{
        long start = System.currentTimeMillis();
        List<OrderTest> list = new ArrayList<>();
        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            conn = PhoenixPool.getInstance().borrowObject();
            statement = conn.createStatement();
            String sql = "select * from "+tableName+" where money > 3000.0";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                OrderTest orderTest = new OrderTest();
                orderTest.setAccount(resultSet.getString("account"));
                orderTest.setId(resultSet.getString("id"));
                orderTest.setMoney(resultSet.getDouble("money"));
                orderTest.setPk(resultSet.getString("pk"));
                orderTest.setTime(resultSet.getString("time"));
                orderTest.setOther(resultSet.getString("other"));
                list.add(orderTest);
            }
            LOG.debug("get took {} ms", System.currentTimeMillis() - start);
            return list;
        }finally {
            if(resultSet != null){
                resultSet.close();
            }
            if(statement != null){
                statement.close();
            }
            PhoenixPool.getInstance().returnObject(conn);
        }
    }
}
