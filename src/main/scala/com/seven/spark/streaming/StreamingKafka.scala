package com.seven.spark.streaming

import java.util.Properties

import com.seven.spark.kafka.KafkaSink
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * author seven
  * time   2018-05-11
  * SparkStreaming向kafka发送消息
  * 接收数据信息
  * seven iphone       mobilePhone
  * stone mi           mobilePhone
  * jack  macBookPro   computer
  * tom   macBookAir   computer
  */
object StreamingKafka {
  private final val log = LoggerFactory.getLogger(this.getClass)
  val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
  val ssc = new StreamingContext(conf, Seconds(5))

  // 广播KafkaSink
  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val brokers = "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092," +
      "vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092"
    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", brokers)
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }
    log.warn("kafka producer init done!")
    ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  def main(args: Array[String]): Unit = {
    val socketData = ssc.socketTextStream("localhost", 7777)
    val topic = "seven"
    socketData.mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(" ")
        list.::=(line(0), line(1) + "," + line(2))
      })
      list.iterator
    }).foreachRDD(x => {
      x.foreachPartition(row => {
        row.foreach(line => {
          kafkaProducer.value.send(topic, line._1, line._2)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
