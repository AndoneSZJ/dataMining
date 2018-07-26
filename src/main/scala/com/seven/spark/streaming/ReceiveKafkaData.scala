package com.seven.spark.streaming

import java.text.SimpleDateFormat

import com.seven.spark.entity.Order
import com.seven.spark.elastic.ElasticOps
import com.seven.spark.hbase.HBaseOps
import com.seven.spark.hbase.rowkey.RowKeyGenerator
import com.seven.spark.hbase.rowkey.generator.HashRowKeyGenerator
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
//隐式转换

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
  * date     2018/5/24 上午11:23     
  */
object ReceiveKafkaData {
  private final val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //topic
    val topics = Array("seven")
    //kafka地址
    val brokers = "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092," +
      "vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092"

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)

    if (args.length == 0 || args == null) {
      sparkConf.setMaster("local[2]")
    }

    //创建streaming对象，5秒计算一次
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //    kafka  0-8
    //    拆分topic
    //    val topicsSet = topics.split(",").toSet
    //    val kafkaParams = Map[String,String](
    //      "metadata.broker.list"-> brokers)
    //    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)

    //    kafka  0-10
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //获取实时数据
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val rowKeyGen: RowKeyGenerator[String] = new HashRowKeyGenerator()
    val family = "INFO"
    kafkaStream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(x => {
          var puts = List[Put]()
          //var orders = List[Order]()
          x.foreach(row => {
            //val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

            val line = row.split(",")
            val rowKey = rowKeyGen.generate("") //获取rowkey
            // 日期，机器编号，状态，支付方式，支付金额，支付账号，支付时间，支付平台，网点，点位，省市区
            // 品项编号，商品名称，商品单价，商品数量，自贩机出货量，退款商品数，机器编号，日期
            val orderId = line(6)
            val money = line(5).toDouble
            val account = line(6)
            val time = line(1)
            val other = row

            val put = new Put(rowKey)
            //列族,列簇,数值
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ID"), Bytes.toBytes(orderId)) //插入id
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("MONEY"), Bytes.toBytes(money)) //插入金额
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("TIME"), Bytes.toBytes(time)) //插入时间
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ACCOUNT"), Bytes.toBytes(account)) //插入用户账号
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("OTHER"), Bytes.toBytes(other)) //插入其他数据
            puts.::=(put)
            //orders.::=(order)
          })
          HBaseOps.puts("ORDERTEST", puts) //HBase工具类，批量插入数据
          log.info(s"Inserting ${puts.size} lines of data to HBase is success . . .")
          //ElasticOps.puts("seven", "order", orders) //ES工具类，批量插入数据
          //log.info(s"Inserting ${orders.size} lines of data to ElasticSearch is success . . .")
        })
      }
    })

    ssc.start() //启动计算
    ssc.awaitTermination()
  }
}
