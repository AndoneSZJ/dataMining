package com.seven.spark.streaming

import java.util

import com.seven.spark.common.Utils
import com.seven.spark.hbase.{HBaseOps, HBaseUtils}
import com.seven.spark.hbase.rowkey.RowKeyGenerator
import com.seven.spark.hbase.rowkey.generator.{FileRowKeyGenerator, HashRowKeyGenerator}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._//隐式转换

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/24 上午11:23     
  */
object ReceiveKafkaData {
  private final val log = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    //topic
    val topics = "seven"
    //kafka地址
    val brokers = "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092," +
      "vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092"

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)

    if(args.length == 0 || args == null){
      sparkConf.setMaster("local[2]")
    }

    //创建streaming对象，5秒计算一次
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //拆分topic
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String,String](
      "metadata.broker.list"-> brokers)
    import org.apache.kafka.common.TopicPartition

    val offsets = new util.HashMap[TopicPartition,Long]()
//    offsets.put(new TopicPartition("seven", 0), 2L)

    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
//    val kafkaStream = KafkaUtils.createDirectStream(ssc,
//      LocationStrategies.PreferConsistent(),
//      ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets))

//    val fileRowKeyGen:RowKeyGenerator[String] = new HashRowKeyGenerator()
    val rowKeyGen:RowKeyGenerator[String] = new HashRowKeyGenerator()
    val family = "family"
    val qualifier = "qualifier"
//    var num = 1
    kafkaStream.map(_._2).foreachRDD(rdd =>{
//      if(rdd.isEmpty()) {
        rdd.foreachPartition(x =>{
          var puts = List[Put]()
          x.foreach(row =>{
            println(row)
            val put = new Put(rowKeyGen.generate(""))//获取rowkey
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(row))//插入一条数据
            puts .::= (put)
          })
//          num += puts.size
//          print(num)
          HBaseOps.put("seven",puts)//工具类，批量插入数据
          log.info("put hbase is success . . .")
        })
//      rdd.foreachPartition(x =>{
//        val hbase = HBaseUtils.getInstance()
//        x.foreach(row => {
//          hbase.put("seven",new String(fileRowKeyGen.generate("")),family,qualifier,row)
//        })
//      })
//      rdd.foreach(x =>{
//        HBaseOps.put("seven",new String(fileRowKeyGen.generate("")),family,qualifier,x)
//      })
//      }
    })

    ssc.start()//启动计算
    ssc.awaitTermination()
  }
}
