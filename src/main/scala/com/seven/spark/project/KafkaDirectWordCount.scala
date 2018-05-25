package com.seven.spark.project

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/19.
  * SparkStream对接KAFKA,Direct方式
  */
object KafkaDirectWordCount {

  def main(args: Array[String]) {

//    if(args.length != 2) {
//      System.err.println("Usage")
//    }

    //val Array(brokers,topics) = args
    val topics = "seven"
    val brokers = "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092," +
      "vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092"

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    //val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
    //val topicsSet = topics.split(",").toSet

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String](
      "metadata.broker.list"-> brokers)


    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
    //val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
