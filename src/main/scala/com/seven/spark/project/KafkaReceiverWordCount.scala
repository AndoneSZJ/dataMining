package com.seven.spark.project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/19.
  * SparkStream对接KAFKA
  */
object KafkaReceiverWordCount {

  def main(args: Array[String]) {

    if(args.length != 4) {
      System.err.println("Usage")
    }

    val Array(zkQuorum,group,topics,numThreads) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    kafkaStream.print()
    kafkaStream.map(_._1).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
