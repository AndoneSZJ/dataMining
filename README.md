# dataMining
Rdd、SparkSql、流式计算、ElasticSearch、HBase、Kafka

数据通过kafka发送，streaming接收处理，将数据储存至HBase，并构建查询条件，将查询条件和rowKey存储至es；获取数据时，通过条件查询es，获取指定rowKey，再通过rowKey查询HBase获取完整数据





phoenix映射hbase中的表，通过sql查询