package com.seven.spark.elastic.pool;

import com.seven.spark.common.PropertiesUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.elasticsearch.client.transport.TransportClient;

import java.util.NoSuchElementException;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    sevenstone@yeah.net
 * date     2018/5/27 上午10:08
 */
public class ElasticPool implements ObjectPool<TransportClient> {

    private GenericObjectPool<TransportClient> pool;

    private static final int MAX_TOTAL;
    private static final int MAX_IDLE;
    private static final int MAX_WAIT_MILLIS;

    static {
        Configuration conf = PropertiesUtils.getConfiguration();
        MAX_TOTAL = conf.getInt("es.pool.maxTotal");
        MAX_IDLE = conf.getInt("es.pool.maxIdle");
        MAX_WAIT_MILLIS = conf.getInt("es.pool.MaxWaitMillis");
    }

    private ElasticPool() {
        pool = new GenericObjectPool<TransportClient>(new PooledElasticFactory());
        pool.setMaxTotal(MAX_TOTAL);
        pool.setMaxIdle(MAX_IDLE);
        pool.setMaxWaitMillis(MAX_WAIT_MILLIS);
        pool.setTimeBetweenEvictionRunsMillis(600000);
        pool.setNumTestsPerEvictionRun(-1);
        pool.setMinEvictableIdleTimeMillis(3000);
    }

    public static ElasticPool getInstance() {
        return ElasticClientPoolHolder.INSTANCE;
    }

    @Override
    public TransportClient borrowObject() throws Exception, NoSuchElementException, IllegalStateException {
        return pool.borrowObject();
    }

    @Override
    public void returnObject(TransportClient client) throws Exception {
        pool.returnObject(client);
    }

    @Override
    public void invalidateObject(TransportClient client) throws Exception {
        pool.invalidateObject(client);
        client.close();
    }

    @Override
    public void addObject() throws Exception, IllegalStateException, UnsupportedOperationException {
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
    public void clear() throws Exception, UnsupportedOperationException {
        pool.clear();
    }

    @Override
    public void close() {
        pool.close();
    }

    private static class ElasticClientPoolHolder {
        private static final ElasticPool INSTANCE = new ElasticPool();

        private ElasticClientPoolHolder() {

        }
    }

}

