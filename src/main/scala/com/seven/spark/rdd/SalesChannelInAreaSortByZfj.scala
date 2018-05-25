package com.seven.spark.rdd

import com.seven.spark.utils.Utils
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    sevenstone@yeah.net
  * date     2018/5/19 上午10:04
  * 大区维度计算渠道下的自贩机销量，数据供给箱线图
  */
object SalesChannelInAreaSortByZfj {

  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info("parameter number is " + args.length)
    log.info("job is start ...")
    val stopWatch = new StopWatch()
    stopWatch.start()
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/seven/SalesMonthAverageByZfj/*"
    log.info("load data is start . . .")
    val rdd = salesSortByArea(path)
    log.info("load data is success . . .")
    //val savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/seven/SalesMonthAverageByZfj/*"
    log.info("save data is start . . .")
    //log.info("savePath is "+ savePath)

    log.info("save data is success . . .")

    val month1 = "2017-05"
    val monthPath1 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month1}/"
    saveDataToHdfs(monthPath1, rdd, month1)
    log.info("save data is success . . .")

    val month2 = "2017-06"
    val monthPath2 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month2}/"
    saveDataToHdfs(monthPath2, rdd, month2)
    log.info("save data is success . . .")

    val month3 = "2017-07"
    val monthPath3 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month3}/"
    saveDataToHdfs(monthPath3, rdd, month3)

    val month4 = "2017-08"
    val monthPath4 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month4}/"
    saveDataToHdfs(monthPath4, rdd, month4)

    val month5 = "2017-09"
    val monthPath5 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month5}/"
    saveDataToHdfs(monthPath5, rdd, month5)

    val month6 = "2017-10"
    val monthPath6 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month6}/"
    saveDataToHdfs(monthPath6, rdd, month6)

    val month7 = "2017-11"
    val monthPath7 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month7}/"
    saveDataToHdfs(monthPath7, rdd, month7)
    log.info("save data is success . . .")

    val month8 = "2017-12"
    val monthPath8 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month8}/"
    saveDataToHdfs(monthPath8, rdd, month8)
    log.info("save data is success . . .")

    val month9 = "2018-01"
    val monthPath9 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month9}/"
    saveDataToHdfs(monthPath9, rdd, month9)

    val month10 = "2018-02"
    val monthPath10 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month10}/"
    saveDataToHdfs(monthPath10, rdd, month10)

    val month11 = "2018-03"
    val monthPath11 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month11}/"
    saveDataToHdfs(monthPath11, rdd, month11)

    val month12 = "2018-04"
    val monthPath12 = s"/Users/seven/data/salesChannelInAreaSortByZfj/${month12}/"
    saveDataToHdfs(monthPath12, rdd, month12)

    stopWatch.stop()
    log.info("job is success . . . \nspend time is " + stopWatch.toString)
  }

  /**
    * 分月份计算数据
    *
    * @param savePath
    * @param rdd
    * @param month
    */
  def saveDataToHdfs(savePath: String, rdd: RDD[String], month: String): Unit = {
    val data = rdd.filter(x => {
      val line = x.split("@")
      line(1).contains(month)
    }).mapPartitions(x => {
      var list = List[(String)]()
      x.foreach(row => {
        val line = row.split("@")
        list.::=(line(0) + "->" + line(2))
      })
      list.iterator
    }).cache()
    //    data.foreach(println)
    //    println("data is " + month)
    data.repartition(1).saveAsTextFile(savePath)
    log.info("save data is success mouth is " + month)


    //val javaRDD = rdd.toJavaRDD()//将rdd转换为javaRdd

    //Utils.saveHdfs(javaRDD,new JavaSparkContext(conf),savePath)//隐式转换调用Java方法

  }

  /**
    * 计算大区下每月不同渠道销量统计
    *
    * @param path
    */
  def salesSortByArea(path: String): RDD[String] = {
    val data = sc.textFile(path).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        //大区-渠道-月份-台销
        val line = row.split(",")
        val area = line(0)
        //大区
        val channel = line(1)
        //渠道
        val month = line(2)
        //月份
        val avgMoney = line(3) //台销
        list.::=(area + "@" + month, channel + "@" + avgMoney)
      })
      list.iterator
    }).reduceByKey(_ + "#" + _).mapPartitions(x => {
      var list = List[String]()
      x.foreach(row => {
        val lines = row._2.split("#")
        var map: Map[String, Double] = Map()
        for (l <- lines) {
          val line = l.split("@")
          val channel = line(0)
          val avgMoney: Double = line(1).toDouble
          map += (channel -> avgMoney)
        }

        //按照value排序，升序      _._1(key排序)
        val channelMap = ListMap(map.toSeq.sortBy(_._2): _*)
        //总数
        val num = channelMap.size
        //平均数
        val avgNum = num / 2
        //判断总数是奇数还是偶数，奇数则取一个渠道，偶数则取两个渠道
        val avgNumber = num % 2
        //10%的人数
        val tenNum = num / 10
        //10%的取余
        val avgTen = num % 10

        var n = 1
        var max = ""
        //销量最好的渠道id
        var min = ""
        //销量最差的渠道id
        var avg = ""
        //销量在中间的渠道id
        var goodTenPercent = ""
        //销量排前百分之十的渠道id
        var badTenPercent = "" //销量排后百分十的渠道id

        for (c <- channelMap) {
          if (n == 1) {
            min += c._1
          }
          if (n == num) {
            max += c._1
          }
          if (n == avgNum && avgNumber == 1) {
            //奇数取一个渠道
            avg += c._1
          }
          if ((n == avgNum || n == avgNum + 1) && avgNumber == 0) {
            //偶数取两个
            avg += c._1 + ","
          }
          if (avgTen > 4) {
            if (n <= tenNum + 1) {
              badTenPercent += c._1 + ","
            }
            if (n > num - (tenNum + 1)) {
              goodTenPercent += c._1 + ","
            }
          } else {
            if (n <= tenNum) {
              badTenPercent += c._1 + ","
            }
            if (n > num - tenNum) {
              goodTenPercent += c._1 + ","
            }
          }
          n += 1
        }

        if (avgNumber == 0) {
          avg = avg.substring(0, avg.length - 1)
        }
        if (goodTenPercent.length > 1) goodTenPercent = goodTenPercent.substring(0, goodTenPercent.length - 1)
        if (badTenPercent.length > 1) badTenPercent = badTenPercent.substring(0, badTenPercent.length - 1)

        //最好的渠道id，排名前百分之十的渠道id，排名中间的渠道id，排名后百分之十的渠道id，最差的渠道id
        val str = s"max:${max};goodTenPercent:${goodTenPercent};avg:${avg};badTenPercent:${badTenPercent};min:${min}"
        //val result = "max:"+max+";goodTenPercent:"+goodTenPercent+";avg:"+avg+";badTenPercent:"+badTenPercent+";min:"+min
        list.::=(row._1 + "@" + str)
      })
      list.iterator
    }).cache()
    data
  }

}
