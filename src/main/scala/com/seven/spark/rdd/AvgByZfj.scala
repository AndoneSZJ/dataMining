package com.seven.spark.rdd

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/22 上午9:01     
  */
object AvgByZfj {
  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info("job is start . . .")
    val stopwatch = new StopWatch()
    stopwatch.start()
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/ZFJ_DATA/*"

    salesAvgByZfj(path)
    stopwatch.stop()
    log.info("job is success . . . ")
    log.info("job is success . . . ")
  }

  /**
    * 计算自贩机渠道销售12月平均值
    *
    * @param path
    */
  def salesAvgByZfj(path: String): Unit = {
    val data = sc.textFile(path).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.replaceAll("[()]", "").split(",")
        val channelName = line(0)
        //去渠道名称
        val month = line(1)
        //月份
        //upper+","+q3+","+midder+","+q1+","+lower
        val upper = line(2)
        //最优
        val q3 = line(3)
        //靠前
        val middle = line(4)
        //中间
        val q1 = line(5)
        //靠后
        val lower = line(6)
        //最差
        val result = month + "," + upper + "," + q3 + "," + middle + "," + q1 + "," + lower
        list.::=(channelName, result)
      })
      list.iterator
    }).reduceByKey(_ + "@" + _).mapPartitions(x => {
      var list = List[(String)]()
      x.foreach(row => {
        val lines = row._2.split("@")
        //获取当前渠道下12月平均值
        var avgUpper = 0.0
        var avgQ3 = 0.0
        var avgMiddle = 0.0
        var avgQ1 = 0.0
        var avgLower = 0.0
        for (l <- lines) {
          val line = l.split(",")
          avgUpper += line(1).toDouble
          avgQ3 += line(2).toDouble
          avgMiddle += line(3).toDouble
          avgQ1 += line(4).toDouble
          avgLower += line(5).toDouble
        }

        avgUpper = avgUpper / 12
        avgQ3 = avgQ3 / 12
        avgMiddle = avgMiddle / 12
        avgQ1 = avgQ1 / 12
        avgLower = avgLower / 12

        list.::=(row._1 + "," + avgUpper + "," + avgQ3 + "," + avgMiddle + "," + avgQ1 + "," + avgLower)

      })
      list.iterator
    }).cache()

    data.repartition(1).saveAsTextFile("/Users/seven/data/avgZfj")
  }

}
