package com.seven.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * author seven
  * time   2018-05-07
  * 接收kafka数据,统计单词数量
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //topic
    val topics = "seven"
    //kafka地址
    val brokers = "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092," +
      "vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092"

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    //创建streaming对象，5秒计算一次
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //拆分topic
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String,String](
      "metadata.broker.list"-> brokers)

//    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
//
//    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()//启动计算
    ssc.awaitTermination()
  }
}
