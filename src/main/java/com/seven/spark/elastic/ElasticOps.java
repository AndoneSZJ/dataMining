package com.seven.spark.elastic;

import com.amazonaws.util.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.seven.spark.entity.Other;
import com.seven.spark.elastic.pool.ElasticPool;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


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
 * date     2018/5/27 上午10:07
 */
public class ElasticOps {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticOps.class);
    public static void main(String[] args) {
        try {
//            Order order = new Order();
//            order.setId("1111");
//            order.setNetId("1111");
//            order.setPointId("1111");
//            ElasticOps.put("","",order);
//            System.out.println(ElasticOps.isIndexTypeExists("megacorp","employee"));
//            TransportClient client = ElasticPool.getInstance().borrowObject();
//            for(DiscoveryNode node : client.connectedNodes())
//            {
//                System.out.println(node.getHostAddress());
//            }
            //ElasticOps.createIndex("data");

//            deleteIndex("data");
            SearchHits searchHits = search("seven","order",0);
            for(SearchHit searchHit : searchHits){
//                System.out.println(searchHit.getSource().get("id"));
//                System.out.println(searchHit.getSource().get("userId"));
//                System.out.println(searchHit.getSource().get("playTime"));
//                System.out.println(searchHit.getSource().get("netId"));
//                System.out.println(searchHit.getSource().get("pointId"));
//                System.out.println(searchHit.getSource().get("rowKey"));
                System.out.println(searchHit.getSourceAsString());
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名称
     * @return 创建成功或已经存在返回true, 否则返回false
     */
    public static boolean createIndex(String indexName) throws Exception {
        return createIndex(indexName, null);
    }

    /**
     * 创建索引并设置索引别名
     *
     * @param indexName 索引名称
     * @param alias     索引别名
     * @return 创建成功或已经存在返回true, 否则返回false
     */
    public static boolean createIndex(String indexName, String alias) throws Exception {
        return createIndex(indexName, alias, -1, -1);
    }

    /**
     * 创建索引并设置索引别名
     *
     * @param indexName 索引名称
     * @param alias     索引别名
     * @param shards    分片数
     * @param replicas  副本数
     * @return 创建成功或已经存在返回true, 否则返回false
     */
    public static boolean createIndex(String indexName, String alias,
                               int shards, int replicas) throws Exception {
        if (isIndexExists(indexName)) {
            LOG.warn("Index [{}] is already EXIST", indexName);
            return true;
        } else {
            TransportClient client = null;
            try {
                client = ElasticPool.getInstance().borrowObject();
                Settings.Builder settingsBuilder = Settings.builder();
                if (shards > 0) {
                    settingsBuilder.put("number_of_shards", shards);
                }
                if (replicas > 0) {
                    settingsBuilder.put("number_of_replicas", replicas);
                }
                Settings settings = settingsBuilder.build();
                CreateIndexRequest request = new CreateIndexRequest(indexName, settings);

                // 创建空索引库
                CreateIndexResponse response = client.admin().indices()
                        .create(request)
                        .actionGet();

                // 添加索引别名
                if (!StringUtils.isEmpty(alias)) {
                    addIndexAlias(indexName, alias);
                }

//                // 设置映射
//                IndicesAdminClient adminClient = client.admin().indices();
//                for (FileType fileType : FileType.getAllTypes()) {
//                    adminClient.preparePutMapping(indexName)
//                            .setType(fileType.getId())
//                            .setSource(getMapping(fileType.getId()))
//                            .execute()
//                            .actionGet();
//                }

                return response.isAcknowledged();
            } finally {
                if (null != client) {
                    ElasticPool.getInstance().returnObject(client);
                }
            }
        }
    }

    /**
     * 设置索引别名
     * @param indexName
     * @param alias
     * @return
     * @throws Exception
     */

    private static boolean addIndexAlias(String indexName, String alias) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            IndicesAliasesResponse response = client.admin().indices()
                    .prepareAliases()
                    .addAlias(indexName, alias)
                    .get();
            return response.isAcknowledged();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }

    }




    private XContentBuilder getMapping(String indexType) throws Exception {
        // index字段
        // analyzed：首先分析这个字符串，然后索引。换言之，以全文形式索引此字段。
        // not_analyzed：索引这个字段，使之可以被搜索，但是索引内容和指定值一样，不分析此字段。
        // no：不索引这个字段，这个字段不能为搜索到。
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(indexType)
                .startObject("properties") // properties为自定义字段

                .startObject("id") // 字段名
                .field("type", "string") // 字段类型
                .field("index", "not_analyzed") // 索引
                .field("store", "no") // 是否存储
                .endObject()

                .startObject("user_id")
                .field("type", "long")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("name")
                .field("type", "string")
                .field("index", "analyzed")
                .field("term_vector", "with_positions_offsets") // 开启向量，用于高亮显示
                .field("analyzer", "ik")
                .endObject()

                .startObject("md5")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("slice_md5")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("type")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("hdfs_path")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("path_id")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("size")
                .field("type", "long")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("split")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("ctime")
                .field("type", "long")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("mtime")
                .field("type", "long")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("dtime")
                .field("type", "long")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("delete_parent_id")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("stime")
                .field("type", "long")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("share_parent_id")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("share_acl")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("share_flag")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("delete_flag")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("text")
                .field("type", "string")
                .field("index", "analyzed")
                .field("term_vector", "with_positions_offsets")
                .field("analyzer", "ik")
                .endObject()

                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName 索引名称
     * @return 存在返回true, 否则返回false
     */
    public static boolean isIndexExists(String indexName) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
            IndicesExistsResponse existsResponse = client.admin().indices()
                    .exists(existsRequest)
                    .actionGet();
            return existsResponse.isExists();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }

    /**
     * 判断索引类型是否存在
     *
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @return 存在返回true, 否则返回false
     */
    public static boolean isIndexTypeExists(String indexName, String indexType) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            TypesExistsResponse response = client.admin().indices()
                    .prepareTypesExists(indexName)
                    .setTypes(indexType)
                    .get();
            return response.isExists();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }

    }

    /**
     * 打开索引
     *
     * @param indexName 索引名称
     */
    public boolean openIndex(String indexName) throws Exception {
        if (!isIndexExists(indexName)) {
            LOG.info("Index [{}] not exists", indexName);
            return false;
        }

        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            OpenIndexRequest request = new OpenIndexRequest(indexName);
            OpenIndexResponse response = client.admin().indices()
                    .open(request)
                    .actionGet();
            return response.isAcknowledged();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }

    }

    /**
     * 关闭索引
     *
     * @param indexName 索引名称
     */
    public boolean closeIndex(String indexName) throws Exception {
        if (!isIndexExists(indexName)) {
            LOG.info("Index [{}] not exists", indexName);
            return false;
        }

        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            CloseIndexRequest request = new CloseIndexRequest(indexName);
            CloseIndexResponse response = client.admin().indices()
                    .close(request)
                    .actionGet();
            return response.isAcknowledged();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }

    }

    /**
     * 删除指定名称的索引
     *
     * @param indexName 索引名称
     * @return 索引存在且删除成功返回true, 否则返回false
     */
    public static boolean deleteIndex(String indexName) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            // 判断索引是否存在，存在则删除
            if (isIndexExists(indexName)) {
                DeleteIndexResponse response = client.admin().indices()
                        .prepareDelete(indexName)
                        .execute()
                        .actionGet();
                return response.isAcknowledged();
            } else {
                LOG.warn("Index [{}] not exist", indexName);
                return false;
            }
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }



    /**
     * 通过JavaBean的方式插入数据
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param t         泛型实体类
     * @throws Exception
     */
    public static <T extends Other> void put(String indexName, String indexType,T t) throws Exception {
        TransportClient client = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            client = ElasticPool.getInstance().borrowObject();
//            IndexResponse indexResponse =
            client.prepareIndex(indexName,indexType,t.getId())
                    .setSource(mapper.writeValueAsString(t)).execute().actionGet();
//            return indexResponse.forcedRefresh();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }

    /**
     * 批量插入JavaBean的数据
     * @param indexName
     * @param indexType
     * @param t
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T extends Other> void puts(String indexName, String indexType, List<T> t) throws Exception {
        TransportClient client = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            client = ElasticPool.getInstance().borrowObject();
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for(int i = 0 ; i < t.size() ; i++){
                bulkRequest.add(client.prepareIndex(indexName,indexType,t.get(i).getId()).setSource(mapper.writeValueAsString(t.get(i))));
                // 每1000条提交一次
                if (i % 1000 == 0) {
                    bulkRequest.execute().actionGet();
                    //此处新建一个bulkRequest，类似于重置效果
                    bulkRequest = client.prepareBulk();
                }
            }
            if(bulkRequest.numberOfActions() > 0){
                bulkRequest.execute().actionGet();
            }
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }


    /**
     * 通过map的方式插入数据
     * @param indexName
     * @param indexType
     * @param map
     * @throws Exception
     */
    public static void put(String indexName, String indexType,String id,Map<String,String> map) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            IndexResponse indexResponse = client.prepareIndex(indexName,indexType,id)
                    .setSource(map).execute().actionGet();
//            return indexResponse.forcedRefresh();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }

    /**
     * 通过json的方式插入数据
     * @param indexName
     * @param indexType
     * @param json
     * @throws Exception
     */
    public static void put(String indexName, String indexType,String id,JSONObject json) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
           client.prepareIndex(indexName,indexType,id)
                    .setSource(json).execute().actionGet();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }

    /**
     * 根据指定的索引名称和索引类型以及id来删除
     *
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @param Id        id
     * @return 存在且删除成功返回true, 否则返回false
     */
    public static  boolean delete(String indexName, String indexType, String Id) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            DeleteResponse response = client.prepareDelete(indexName, indexType, Id).get();
            return response.forcedRefresh();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }

    }


    /**
     * 查询订单
     * @param indexName
     * @param indexType
     * @param num      分页查询的页数(第几页)
     * @param strings  用户id 网点id 点位id 支付时间开始时间，结束时间
     * @return
     * @throws Exception
     */
    public static SearchHits search(String indexName, String indexType,int num, String... strings) throws Exception {
        TransportClient client = null;
        try {
            client = ElasticPool.getInstance().borrowObject();
            SearchRequestBuilder requestBuilder = client.prepareSearch(indexName)
                    .setTypes(indexType)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
//                    .setQuery(queryBuilder)
//                    .execute()
//                    .actionGet();

            if(strings.length > 0){
                String userId = strings[0];
                String pointId = strings[1];
                String netId = strings[2];
                String startTime = strings[3];
                String endTime = strings[4];

                if(!"".equals(startTime) && !"".equals(endTime)){
                    QueryBuilder playTimeBuilder = QueryBuilders.rangeQuery("playTime")
                            .gt(startTime)//大于开始时间
                            .lt(endTime)//小于结束时间
                            .includeLower(true)//包括下界
                            .includeUpper(true);//包括上界
                    requestBuilder.setQuery(playTimeBuilder);
                }

                if(!"".equals(userId)){
                    QueryBuilder userIdBuilder = QueryBuilders.termQuery("userId",userId);
                    requestBuilder.setQuery(userIdBuilder);
                }
                if(!"".equals(pointId)){
                    QueryBuilder pointIdBuilder = QueryBuilders.termQuery("pointId",pointId);
                    requestBuilder.setQuery(pointIdBuilder);
                }
                if(!"".equals(netId)){
                    QueryBuilder netIdBuilder = QueryBuilders.termQuery("netId",netId);
                    requestBuilder.setQuery(netIdBuilder);
                }
            }



            SearchResponse response = requestBuilder
                    .addSort("playTime",SortOrder.DESC)//按照支付时间排序
                    .setFrom(num*10)//跳过多少行
                    .setSize(100)//设置分页数量
                    .execute().actionGet();

            System.out.println(response.getHits().totalHits());
            return response.getHits();
        } finally {
            if (null != client) {
                ElasticPool.getInstance().returnObject(client);
            }
        }
    }


}
