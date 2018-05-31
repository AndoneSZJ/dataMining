package com.seven.spark.elastic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.seven.spark.elastic.pool.ElasticPool;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 封装ElasticSearch操作
 *
 * @author songyougang
 */
public class ElasticOperation {
//
//    private static final Logger LOG = LoggerFactory.getLogger(ElasticOperation.class);
//
//    /**
//     * 创建索引
//     *
//     * @param indexName 索引名称
//     * @return 创建成功或已经存在返回true, 否则返回false
//     */
//    public boolean createIndex(String indexName) throws Exception {
//        return createIndex(indexName, null);
//    }
//
//    /**
//     * 创建索引并设置索引别名
//     *
//     * @param indexName 索引名称
//     * @param alias     索引别名
//     * @return 创建成功或已经存在返回true, 否则返回false
//     */
//    public boolean createIndex(String indexName, String alias) throws Exception {
//        return createIndex(indexName, alias, -1, -1);
//    }
//
//    /**
//     * 创建索引并设置索引别名
//     *
//     * @param indexName 索引名称
//     * @param alias     索引别名
//     * @param shards    分片数
//     * @param replicas  副本数
//     * @return 创建成功或已经存在返回true, 否则返回false
//     */
//    public boolean createIndex(String indexName, String alias,
//                               int shards, int replicas) throws Exception {
//        if (isIndexExists(indexName)) {
//            LOG.warn("Index [{}] is already EXIST", indexName);
//            return true;
//        } else {
//            TransportClient client = null;
//            try {
//                client = ElasticPool.getInstance().borrowObject();
//                Settings.Builder settingsBuilder = Settings.settingsBuilder();
//                if (shards > 0) {
//                    settingsBuilder.put("number_of_shards", shards);
//                }
//                if (replicas > 0) {
//                    settingsBuilder.put("number_of_replicas", replicas);
//                }
//                Settings settings = settingsBuilder.build();
//                CreateIndexRequest request = new CreateIndexRequest(indexName, settings);
//
//                // 创建空索引库
//                CreateIndexResponse response = client.admin().indices()
//                        .create(request)
//                        .actionGet();
//
//                // 添加索引别名
//                if (!StringUtils.isEmpty(alias)) {
//                    addIndexAlias(indexName, alias);
//                }
//
//                // 设置映射
//                IndicesAdminClient adminClient = client.admin().indices();
//                for (HdfsProtos.HdfsFileStatusProto.FileType fileType : FileType.getAllTypes()) {
//                    adminClient.preparePutMapping(indexName)
//                            .setType(fileType.getId())
//                            .setSource(getMapping(fileType.getId()))
//                            .execute()
//                            .actionGet();
//                }
//
//                return response.isAcknowledged();
//            } finally {
//                if (null != client) {
//                    ElasticPool.getInstance().returnObject(client);
//                }
//            }
//        }
//    }
//
//    public boolean createIndexWithMapping(String indexName, String indexType, String alias,
//                                          XContentBuilder mappingBuilder) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(indexName);
//            if (null != mappingBuilder) {
//                builder.addMapping(indexType, mappingBuilder);
//            }
//            CreateIndexResponse response = builder.get();
////            CreateIndexRequest request = new CreateIndexRequest(indexName, settings);
////            CreateIndexResponse response = client.admin().indices()
////                    .create(request)
////                    .actionGet();
//
//            // 添加索引别名
//            if (!StringUtils.isEmpty(alias)) {
//                addIndexAlias(indexName, alias);
//            }
//
//            return response.isAcknowledged();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    private boolean addIndexAlias(String indexName, String alias) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            IndicesAliasesResponse response = client.admin().indices()
//                    .prepareAliases()
//                    .addAlias(indexName, alias)
//                    .get();
//            return response.isAcknowledged();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    private XContentBuilder getMapping(String indexType) throws Exception {
//        // index字段
//        // analyzed：首先分析这个字符串，然后索引。换言之，以全文形式索引此字段。
//        // not_analyzed：索引这个字段，使之可以被搜索，但是索引内容和指定值一样，不分析此字段。
//        // no：不索引这个字段，这个字段不能为搜索到。
//        return XContentFactory.jsonBuilder()
//                .startObject()
//                .startObject(indexType)
//
////                .startObject("_all")
////                .field("analyzer", "ik")
////                .endObject()
//
//                .startObject("properties") // properties为自定义字段
//
//                .startObject("id") // 字段名
//                .field("type", "string") // 字段类型
//                .field("index", "not_analyzed") // 索引
//                .field("store", "no") // 是否存储
//                .endObject()
//
//                .startObject("user_id")
//                .field("type", "long")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("name")
//                .field("type", "string")
//                .field("index", "analyzed")
//                .field("term_vector", "with_positions_offsets") // 开启向量，用于高亮显示
//                .field("analyzer", "ik")
////                .field("indexAnalyzer", "ik")
////                .field("searchAnalyzer", "ik")
//                .endObject()
//
//                .startObject("md5")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("slice_md5")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("type")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("hdfs_path")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("path_id")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("size")
//                .field("type", "long")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("split")
//                .field("type", "integer")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("ctime")
//                .field("type", "long")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("mtime")
//                .field("type", "long")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("dtime")
//                .field("type", "long")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("delete_parent_id")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("stime")
//                .field("type", "long")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("share_parent_id")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("share_acl")
//                .field("type", "string")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("share_flag")
//                .field("type", "integer")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("delete_flag")
//                .field("type", "integer")
//                .field("index", "not_analyzed")
//                .endObject()
//
//                .startObject("text")
//                .field("type", "string")
//                .field("index", "analyzed")
//                .field("term_vector", "with_positions_offsets")
//                .field("analyzer", "ik")
//                .endObject()
//
//                .endObject()
//                .endObject()
//                .endObject();
//    }
//
//    /**
//     * 判断索引是否存在
//     *
//     * @param indexName 索引名称
//     * @return 存在返回true, 否则返回false
//     */
//    public boolean isIndexExists(String indexName) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
//            IndicesExistsResponse existsResponse = client.admin().indices()
//                    .exists(existsRequest)
//                    .actionGet();
//            return existsResponse.isExists();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
//
//    /**
//     * 判断索引类型是否存在
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @return 存在返回true, 否则返回false
//     */
//    public boolean isIndexTypeExists(String indexName, String indexType) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            TypesExistsResponse response = client.admin().indices()
//                    .prepareTypesExists(indexName)
//                    .setTypes(indexType)
//                    .get();
//            return response.isExists();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    /**
//     * 打开索引
//     *
//     * @param indexName 索引名称
//     */
//    public boolean openIndex(String indexName) throws Exception {
//        if (!isIndexExists(indexName)) {
//            LOG.info("Index [{}] not exists", indexName);
//            return false;
//        }
//
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            OpenIndexRequest request = new OpenIndexRequest(indexName);
//            OpenIndexResponse response = client.admin().indices()
//                    .open(request)
//                    .actionGet();
//            return response.isAcknowledged();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    /**
//     * 关闭索引
//     *
//     * @param indexName 索引名称
//     */
//    public boolean closeIndex(String indexName) throws Exception {
//        if (!isIndexExists(indexName)) {
//            LOG.info("Index [{}] not exists", indexName);
//            return false;
//        }
//
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            CloseIndexRequest request = new CloseIndexRequest(indexName);
//            CloseIndexResponse response = client.admin().indices()
//                    .close(request)
//                    .actionGet();
//            return response.isAcknowledged();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    /**
//     * 索引文档到指定类型的索引中
//     *
//     * @param indexName     索引名称
//     * @param indexType     索引类型
//     * @param docId         文档id
//     * @param sourceBuilder 文档信息
//     * @throws IOException 如果索引文档错误
//     */
//    public void indexDoc(
//            String indexName,
//            String indexType,
//            String docId,
//            XContentBuilder sourceBuilder
//    ) throws Exception {
//
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            long start = System.currentTimeMillis();
//            IndexRequestBuilder builder;
//            if (!StringUtils.isEmpty(docId)) {
//                builder = client.prepareIndex(indexName, indexType, docId);
//            } else {
//                builder = client.prepareIndex(indexName, indexType);
//            }
//
//            builder.setSource(sourceBuilder)
//                    .execute()
//                    .actionGet();
//            LOG.debug("index doc took {} ms", System.currentTimeMillis() - start);
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    /**
//     * 批量索引文档到指定类型的索引中
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @param sources   文档信息
//     */
//    public void indexDoc(
//            String indexName,
//            String indexType,
//            List<XContentBuilder> sources
//    ) throws Exception {
//        indexDoc(indexName, indexType, null, sources);
//    }
//
//    /**
//     * 批量索引文档到指定类型的索引中
//     *
//     * @param indexName   索引名称
//     * @param indexType   索引类型
//     * @param builderToId 文档id列表
//     * @param sources     文档信息
//     */
//    public void indexDoc(
//            String indexName,
//            String indexType,
//            Map<XContentBuilder, String> builderToId,
//            List<XContentBuilder> sources
//    ) throws Exception {
//        TransportClient client = null;
//        try {
//            long start = System.currentTimeMillis();
//            client = ElasticPool.getInstance().borrowObject();
//            BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//            int count = 0;
//            int batchSize = sources.size();
//
//            if (null != builderToId && !builderToId.isEmpty()) {
//                for (XContentBuilder source : sources) {
//                    bulkRequest.add(client.prepareIndex(indexName, indexType, builderToId.get(source)).setSource(source));
//                    count++;
//                    // 每个批次大小提交一次
//                    if (count % batchSize == 0) {
//                        LOG.info("commit actions: " + bulkRequest.numberOfActions());
//                        bulkRequest.execute().actionGet();
//                        bulkRequest = client.prepareBulk();
//                    }
//                }
//            } else {
//                for (XContentBuilder source : sources) {
//                    bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(source));
//                    count++;
//                    // 每个批次大小提交一次
//                    if (count % batchSize == 0) {
//                        LOG.info("commit actions: " + bulkRequest.numberOfActions());
//                        bulkRequest.execute().actionGet();
//                        bulkRequest = client.prepareBulk();
//                    }
//                }
//            }
//
//            // 有操作才提交
//            if (bulkRequest.numberOfActions() > 0) {
//                LOG.info("commit action: " + bulkRequest.numberOfActions());
//                bulkRequest.execute().actionGet();
//            }
//            LOG.info("index number of {} docs into es took {} ms", count, System.currentTimeMillis() - start);
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    /**
//     * 批量索引文档到指定类型的索引中
//     *
//     * @param indexName   索引名称
//     * @param indexType   索引类型
//     * @param sources     文档信息
//     */
//    public List<String> indexDocWithReturnDocId(
//            String indexName,
//            String indexType,
//            List<XContentBuilder> sources
//    ) throws Exception {
//        List<String> docIds = new ArrayList<String>();
//        TransportClient client = null;
//        try {
//            long start = System.currentTimeMillis();
//            client = ElasticPool.getInstance().borrowObject();
//            BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//            int count = 0;
//            int batchSize = sources.size();
//
//            for (XContentBuilder source : sources) {
//                bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(source));
//                count++;
//                // 每个批次大小提交一次
//                if (count % batchSize == 0) {
//                    LOG.info("commit actions: " + bulkRequest.numberOfActions());
//                    BulkResponse response = bulkRequest.execute().actionGet();
//                    BulkItemResponse[] items = response.getItems();
//                    for (BulkItemResponse item : items) {
//                        docIds.add(item.getId());
//                    }
//                    bulkRequest = client.prepareBulk();
//                }
//            }
//
//            // 有操作才提交
//            if (bulkRequest.numberOfActions() > 0) {
//                LOG.info("commit action: " + bulkRequest.numberOfActions());
//                BulkResponse response = bulkRequest.execute().actionGet();
//                BulkItemResponse[] items = response.getItems();
//                for (BulkItemResponse item : items) {
//                    docIds.add(item.getId());
//                }
//            }
//            LOG.info("index number of {} docs into es took {} ms", count, System.currentTimeMillis() - start);
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//        return docIds;
//    }
//
//    /**
//     * 判断索引是否存在
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @param docId     文档id
//     */
//    public Map<String, Object> getDoc(String indexName, String indexType, String docId)
//            throws Exception {
//        return getDoc(indexName, indexType, docId, null);
//    }
//
//    /**
//     * 判断索引是否存在
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @param docId     文档id
//     * @param fields    字段列表
//     */
//    public Map<String, Object> getDoc(String indexName, String indexType,
//                                      String docId, String... fields) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//
//            GetRequestBuilder builder = client.prepareGet(indexName, indexType, docId);
//            GetResponse getResponse;
//
//            if (null == fields) {
//                getResponse = builder.get();
//                Map<String, Object> doc = getResponse.getSourceAsMap();
//                if (null == doc) {
//                    return Collections.emptyMap();
//                }
//                doc.put("doc_id", getResponse.getId());
//                return doc;
//            }
//
//            getResponse = builder.setFields(fields).get();
//
//            Map<String, Object> doc = new HashMap<String, Object>(fields.length);
//            for (String name : fields) {
//                GetField getField = getResponse.getField(name);
//                doc.put(getField.getName(), getField.getValue());
//            }
//
//            return doc;
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
//
//    /**
//     * 根据指定的索引名称和索引类型以及文档id来更新文档内容
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @param docId     文档id
//     * @param name      字段名
//     * @param value     字段值
//     * @return 更新成功返回true, 否则返回false
//     */
//    public boolean updateDoc(String indexName, String indexType,
//                             String docId, String name, Object value) throws Exception {
//        Map<String, Object> fields = Maps.newHashMap();
//        fields.put(name, value);
//        return updateDoc(indexName, indexType, docId, fields);
//    }
//
//    /**
//     * 根据指定的索引名称和索引类型以及文档id来更新文档内容
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @param docId     文档id
//     * @param fields    字段集合
//     * @return 更新成功返回true, 否则返回false
//     */
//    public boolean updateDoc(String indexName, String indexType,
//                             String docId, Map<String, Object> fields) throws Exception {
//        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
//        jsonBuilder.startObject();
//        for (Map.Entry<String, Object> entry : fields.entrySet()) {
//            jsonBuilder.field(entry.getKey(), entry.getValue());
//        }
//        jsonBuilder.endObject();
//        return updateDoc(indexName, indexType, docId, jsonBuilder);
//    }
//
//    public void bulkUpdate(String indexName, String indexType,
//                           List<String> docIds, Map<String, Object> fields) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
//            jsonBuilder.startObject();
//            for (Map.Entry<String, Object> entry : fields.entrySet()) {
//                jsonBuilder.field(entry.getKey(), entry.getValue());
//            }
//            jsonBuilder.endObject();
//
//            for (String docId : docIds) {
//                bulkRequest.add(client.prepareUpdate(indexName, indexType, docId)
//                        .setDoc(jsonBuilder).request());
//            }
//            BulkResponse bulkResponse = bulkRequest.get();
//            if (bulkResponse.hasFailures()) {
//                for (BulkItemResponse item : bulkResponse.getItems()) {
//                    LOG.error(item.getFailureMessage());
//                }
//            }
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
//
//    public void bulkUpdateWithTypes(String indexName, Map<String, String> idToType,
//                                    List<String> docIds, Map<String, Object> fields) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
//            jsonBuilder.startObject();
//            for (Map.Entry<String, Object> entry : fields.entrySet()) {
//                jsonBuilder.field(entry.getKey(), entry.getValue());
//            }
//            jsonBuilder.endObject();
//
//            for (String docId : docIds) {
//                bulkRequest.add(client.prepareUpdate(indexName, idToType.get(docId), docId)
//                        .setDoc(jsonBuilder).request());
//            }
//            BulkResponse bulkResponse = bulkRequest.get();
//            if (bulkResponse.hasFailures()) {
//                for (BulkItemResponse item : bulkResponse.getItems()) {
//                    LOG.error(item.getFailureMessage());
//                }
//            }
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
//
//    /**
//     * 根据指定的索引名称和索引类型以及文档id来更新文档内容
//     *
//     * @param indexName   索引名称
//     * @param indexType   索引类型
//     * @param docId       文档id
//     * @param jsonBuilder 内容构建器
//     * @return 更新成功返回true, 否则返回false
//     */
//    public boolean updateDoc(String indexName, String indexType,
//                             String docId, XContentBuilder jsonBuilder) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            client.prepareUpdate(indexName, indexType, docId)
//                    .setDoc(jsonBuilder)
//                    .get();
//            return true;
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
//
//    /**
//     * 根据指定的索引名称和索引类型以及文档id来删除文档
//     *
//     * @param indexName 索引名称
//     * @param indexType 索引类型
//     * @param docId     文档id
//     * @return 文档存在且删除成功返回true, 否则返回false
//     */
//    public boolean deleteDoc(String indexName, String indexType, String docId) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            DeleteResponse response = client.prepareDelete(indexName, indexType, docId).get();
//            return response.isFound();
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    public Result search(String index, String type,
//                         QueryBuilder query) throws Exception {
//        return search(index, type, query, null);
//    }
//
//    public Result search(String index, String type,
//                         QueryBuilder query, Page page) throws Exception {
//        return search(index, type, query, null, page);
//    }
//
//    public Result search(String index, String type, QueryBuilder query,
//                         SortBuilder sortBuilder, Page page) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            long start = 0;
//            if (LOG.isDebugEnabled()) {
//                start = System.currentTimeMillis();
//            }
//
//            SearchRequestBuilder builder = client.prepareSearch(index);
//            if (!StringUtils.isEmpty(type)) {
//                builder.setTypes(type);
//            }
//
//            builder.setQuery(query);
//
//            // 分页查询
//            if (null != page) {
//                builder.setFrom((page.getPage() - 1) * page.getPageSize())
//                        .setSize(page.getPageSize());
//            } else {
//                builder.setSize(10000); // 默认只返回10条，最多10000条
//            }
//
//            // 字段排序
//            if (null != sortBuilder) {
//                builder.addSort(sortBuilder);
////            .addSort("_score", SortOrder.DESC)
//            }
//
//            // 执行搜索
//            SearchResponse response = builder.execute().actionGet();
//
//            SearchHits hits = response.getHits();
//            LOG.debug("HITS: " + hits.getTotalHits());
//            for (SearchHit hit : hits) {
//                LOG.debug(hit.getSourceAsString());
//            }
//
//            List<Map<String, Object>> rows = Lists.newLinkedList();
//            for (SearchHit hit : hits) {
//                Map<String, Object> source = hit.getSource();
//                source.put("doc_id", hit.getId());
//                rows.add(source);
//            }
//            Result result = new Result();
//            result.setHits(hits.getTotalHits());
//            result.setResults(rows);
//            LOG.debug("search took " + (System.currentTimeMillis() - start) + " ms");
//            return result;
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    public Result searchWithHighlight(String index, String type,
//                                      QueryBuilder query, Page page, List<String> fields) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            long start = 0;
//            if (LOG.isDebugEnabled()) {
//                start = System.currentTimeMillis();
//            }
//
//            // 设置索引和类型
//            SearchRequestBuilder builder = client.prepareSearch(index);
//            if (!StringUtils.isEmpty(type)) {
//                builder.setTypes(type);
//            }
//
//            // 分页查询
//            if (null != page) {
//                builder.setFrom((page.getPage() - 1) * page.getPageSize())
//                        .setSize(page.getPageSize());
//            } else {
//                builder.setSize(10000); // 默认只返回10条，最多10000条
//            }
//
//            builder.setQuery(query); // 设置查询对象
//            builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // 搜索类型
//
//            // 设置高亮字段
//            if (null != fields) {
//                for (String field : fields) {
//                    builder.addHighlightedField(field);
//                }
//            }
//            // 设置高亮标签
//            builder.setHighlighterPreTags("<span style=\"color:red\">")
//                    .setHighlighterPostTags("</span>");
////                .setHighlighterRequireFieldMatch(false)
//
//            builder.setExplain(true); // 设置是否按查询匹配度排序
//
//            // 执行搜索
//            SearchResponse response = builder.execute().actionGet();
//
//            SearchHits searchHits = response.getHits();
//            LOG.debug("HITS: " + searchHits.getTotalHits());
//            SearchHit[] hits = searchHits.getHits();
//
//            List<Map<String, Object>> rows = Lists.newLinkedList();
//            for (SearchHit hit : hits) {
//                Map<String, Object> source = hit.getSource();
//                source.put("doc_id", hit.getId());
//                rows.add(source);
//
//                Map<String, HighlightField> result = hit.highlightFields();
//                if (null != fields) {
//                    for (String field : fields) {
//                        HighlightField highlightField = result.get(field);
//                        if (null != highlightField) {
//                            // 获取定义的高亮标签
//                            Text[] fragments = highlightField.fragments();
//                            StringBuilder sb = new StringBuilder();
//                            for (Text text : fragments) {
//                                sb.append(text);
//                            }
//
//                            source.put(field, sb.toString());
//                            LOG.debug("Highlight: " + sb.toString());
//                        }
//                    }
//                }
//            }
//
//            Result result = new Result();
//            result.setHits(hits.length);
//            result.setResults(rows);
//            LOG.debug("search took " + (System.currentTimeMillis() - start) + " ms");
//            return result;
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//
//    }
//
//    /**
//     * 删除指定名称的索引
//     *
//     * @param indexName 索引名称
//     * @return 索引存在且删除成功返回true, 否则返回false
//     */
//    public boolean deleteIndex(String indexName) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            // 判断索引是否存在，存在则删除
//            if (isIndexExists(indexName)) {
//                DeleteIndexResponse response = client.admin().indices()
//                        .prepareDelete(indexName)
//                        .execute()
//                        .actionGet();
//                return response.isAcknowledged();
//            } else {
//                LOG.warn("Index [{}] not exist", indexName);
//                return false;
//            }
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
//
//    public void bulkDelete(String indexName, String indexType, List<String> docIds) throws Exception {
//        TransportClient client = null;
//        try {
//            client = ElasticPool.getInstance().borrowObject();
//            BulkRequestBuilder bulkRequest = client.prepareBulk();
//            for (String docId : docIds) {
//                bulkRequest.add(client.prepareDelete(indexName, indexType, docId).request());
//            }
//            BulkResponse bulkResponse = bulkRequest.get();
//            if (bulkResponse.hasFailures()) {
//                for (BulkItemResponse item : bulkResponse.getItems()) {
//                    LOG.error(item.getFailureMessage());
//                }
//            }
//        } finally {
//            if (null != client) {
//                ElasticPool.getInstance().returnObject(client);
//            }
//        }
//    }
}
