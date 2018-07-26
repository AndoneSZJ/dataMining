package com.seven.spark.phoenix.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    sevenstone@yeah.net
 * date     2018/7/24 上午11:20
 */
public class PooledPhoenixFactory extends BasePooledObjectFactory<Connection> {
    public static final Logger LOG = LoggerFactory.getLogger(PooledPhoenixFactory.class);

    private static String jdbcUrl = "jdbc:phoenix:vm-xaj-bigdata-da-d01,vm-xaj-bigdata-da-d02,vm-xaj-bigdata-da-d03";

    @Override
    public Connection create() throws Exception {
        long start = System.currentTimeMillis();
        Connection conn = null;
        // 创建phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(jdbcUrl);
        LOG.debug("Get phoenix connection took {} ms", System.currentTimeMillis() - start);
        return conn;
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        p.getObject().close();
        super.destroyObject(p);
    }
}
