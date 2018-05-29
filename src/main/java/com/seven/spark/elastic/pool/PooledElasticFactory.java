package com.seven.spark.elastic.pool;

import com.seven.spark.common.PropertiesUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    sevenstone@yeah.net
 * date     2018/5/27 上午10:09
 */
public class PooledElasticFactory extends BasePooledObjectFactory<TransportClient> {
    private static final Logger LOG = LoggerFactory.getLogger(PooledElasticFactory.class);

    private static String[] hosts;
    private static int port;
    private static Settings settings;

    static {
        Configuration conf = PropertiesUtils.getConfiguration();
        hosts = conf.getStringArray("es.node.list");
        port = conf.getInt("es.tcp.port");
        String clusterName = conf.getString("es.cluster.name");
        boolean enableSniff = conf.getBoolean("es.client.transport.sniff");
        LOG.debug("host: {}, port: {}, cluster name: {}, enable sniff: {}", hosts, port, clusterName, enableSniff);

        settings = Settings.builder()
                //去除jar冲突
                //https://discuss.elastic.co/t/netty-dependency-issue-with-elasticsearch-5-4/89120/9
                .put("transport.type","netty3")
                .put("http.type", "netty3")
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", enableSniff)
                .build();
    }

    @Override
    public TransportClient create() throws Exception {
        long start = System.currentTimeMillis();

        TransportClient client = new PreBuiltTransportClient(settings);//.addTransportAddress(hosts,port);
//                TransportClient
//                .builder().settings(settings).build();
        for (String host : hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        }
        LOG.debug("Get es client took {} ms", System.currentTimeMillis() - start);
        return client;
    }

    @Override
    public PooledObject<TransportClient> wrap(TransportClient client) {
        return new DefaultPooledObject<TransportClient>(client);
    }

    @Override
    public void destroyObject(PooledObject<TransportClient> p) throws Exception {
        p.getObject().close();
        super.destroyObject(p);
    }
}

