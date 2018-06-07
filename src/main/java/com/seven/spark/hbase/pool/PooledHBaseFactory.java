package com.seven.spark.hbase.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
 * date     2018/5/11 上午9:37
 */
public class PooledHBaseFactory extends BasePooledObjectFactory<Connection> {
    private static final Logger LOG = LoggerFactory.getLogger(PooledHBaseFactory.class);

    @Override
    public Connection create() throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get hbase connection took {} ms", System.currentTimeMillis() - start);
        }
        return conn;
    }

    @Override
    public PooledObject<Connection> wrap(Connection conn) {
        return new DefaultPooledObject<Connection>(conn);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        p.getObject().close();
        super.destroyObject(p);
    }
}
