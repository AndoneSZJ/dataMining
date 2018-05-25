package com.seven.spark.util

import java.text.DecimalFormat
import java.util
import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/24 上午11:03     
  */
object SimulationKafkaSendOutData {
  def main(args: Array[String]): Unit = {
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order/*"
    val map = getDataByOrder(path)
    simulationSendOutData(map)
  }

  def getDataByOrder(path: String): util.HashMap[Int,String] ={
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName))
    val data = sc.textFile(path).collect()
    val map = new util.HashMap[Int,String]()
    var num = 1
    for(d <- data){
      map.put(num,num+","+d)
      num += 1
    }
    sc.stop()
    map
  }

  def simulationSendOutData(map: util.HashMap[Int,String]): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers","vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092,vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092")
    props.put("acks","all")
    props.put("retries","0")
    props.put("batch.size","16384")
    props.put("linger.ms","1")
    props.put("buffer.memory","33554432")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    for(m <- 1 to 100){
      val message = map.get(m)
      producer.send(new ProducerRecord[String, String]("seven", m.toString,message))
      println(message)
//      if(m % 5 == 0){
        Thread.sleep(500)
//      }
    }
    producer.close()
  }
}
