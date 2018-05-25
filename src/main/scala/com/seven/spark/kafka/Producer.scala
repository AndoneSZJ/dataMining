package com.seven.spark.kafka

import java.text.DecimalFormat
import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * author seven
  * 生产者，模拟发数据
  */
object Producer {
  def main(args: Array[String]): Unit = {
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

    val users = Array("jack","leo","andy","lucy","jim","smith","iverson","andrew")
    val pages = Array("iphone4.html","huawei.html","mi.html","mac.html","note.html","book.html","fanlegefan.com")
    val df = new DecimalFormat("#.00")
    val random = new Random()
    val num = 10000
    for(i<- 0 to num ){
      val message = users(random.nextInt(users.length))+" "+pages(random.nextInt(pages.length))+
        " "+df.format(random.nextDouble()*1000)+" "+System.currentTimeMillis()
      producer.send(new ProducerRecord[String, String]("seven", Integer.toString(i),message))
      println(message)
      Thread.sleep(1000)
    }
    producer.close()
  }
}
