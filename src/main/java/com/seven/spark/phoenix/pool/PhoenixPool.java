package com.seven.spark.phoenix.pool;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.sql.Connection;
import java.util.NoSuchElementException;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    zjshi01@mail.nfsq.com.cn
 * date     2018/7/24 上午11:22
 */
public class PhoenixPool implements ObjectPool<Connection> {

    private GenericObjectPool<Connection> pool;

    private PhoenixPool() {
        pool = new GenericObjectPool<Connection>(new PooledPhoenixFactory());
        // 最大连接数
        pool.setMaxTotal(100);
        // 池中可以空闲对象的最大数目
        pool.setMaxIdle(10);
        // 对象池空时调用borrowObject方法，最多等待多少毫秒
        pool.setMaxWaitMillis(100);
        // 每隔多少毫秒进行一次后台对象清理的行动
        pool.setTimeBetweenEvictionRunsMillis(600000);
        // -1表示清理时检查所有线程
        pool.setNumTestsPerEvictionRun(-1);
        // 设定在进行后台对象清理时，休眠时间超过了3000毫秒的对象为过期
        pool.setMinEvictableIdleTimeMillis(3000);
    }

    public static PhoenixPool getInstance(){
        return PhoenixClientPool.INSTANCE;
    }

    @Override
    public Connection borrowObject() throws Exception {
        return pool.borrowObject();
    }

    @Override
    public void returnObject(Connection connection) throws Exception {
        pool.returnObject(connection);
    }

    @Override
    public void invalidateObject(Connection connection) throws Exception {
        pool.invalidateObject(connection);
        connection.close();
    }

    @Override
    public void addObject() throws Exception {
        pool.addObject();
    }

    @Override
    public int getNumIdle() {
        return pool.getNumIdle();
    }

    @Override
    public int getNumActive() {
        return pool.getNumActive();
    }


    @Override
    public void clear() throws Exception {
        pool.clear();
    }

    @Override
    public void close() {
        pool.close();
    }

    private static class PhoenixClientPool {
        private static final PhoenixPool INSTANCE = new PhoenixPool();

        private PhoenixClientPool() {

        }
    }
}
