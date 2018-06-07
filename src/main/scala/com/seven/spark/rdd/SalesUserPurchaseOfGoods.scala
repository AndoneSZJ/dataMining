package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

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
  * date     2018/5/11 上午10:56
  *
  */
object SalesUserPurchaseOfGoods {

  private final val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val stopWatch = new StopWatch()
    stopWatch.start()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    log.info(this.getClass.getSimpleName + "\t is start . . .")

    val orderPath = "/yst/vem/sales/order/*"

    val rdd = salesOrderByUser(sc, orderPath)

    val userAllPath = "/yst/seven/data/user/all/"

    salesUseAll(userAllPath, rdd)

    val userPath = "/yst/seven/data/user/day/"

    salesUser(userPath, rdd)
    stopWatch.stop()
    log.info(this.getClass.getSimpleName + "\t is success . . .")
    log.info("job time is " + stopWatch.toString)

  }

  /**
    * 获取五十日内的销量
    *
    * @param path
    * @return
    */
  def salesOrderByUser(sc: SparkContext, path: String): RDD[String] = {
    val time = 1000 * 3600 * 24 * 50L
    //计算五十天之内
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    val user = sc.textFile(path).filter(x => {
      val line = x.split(",")
      ((date.getTime - df.parse(line(0)).getTime) < time) && (!"100.0".equals(line(4))) //过滤五十天之外的数据
    }).cache()
    user
  }

  /**
    * 计算五十天内，用户购买商品总数，过滤只购买过一种商品数的用户
    *
    * @param path 存储位置
    */
  def salesUseAll(path: String, rdd: RDD[String]): Unit = {
    val user = rdd.mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        list.::=(line(5), line(11))
      })
      list.iterator
    }).reduceByKey(_ + "," + _)
      .filter(x => {
        val line = x._2.split(",")
        var map: Map[String, String] = Map()
        for (l <- line) {
          if (!map.contains(l)) {
            map += (l -> l)
          }
        }
        map.size > 1
      }) //过滤购买过一次的商品
      .mapPartitions(x => {
      var list = List[(String)]()
      x.foreach(row => {
        val line = row._2.split(",")
        var map: Map[String, String] = Map()
        for (l <- line) {
          if (!map.contains(l)) {
            map += (l -> l)
          }
        }
        var str = ""
        for (m <- map) {
          str += m._1 + ","
        }
        list.::=(str.substring(0, str.length - 1))
      })
      list.iterator
    }).cache()

    user.repartition(1).saveAsTextFile(path)

  }

  /**
    * 计算五十天内，每天用户购买超过一次的商品
    *
    * @param path
    * @param rdd
    */
  def salesUser(path: String, rdd: RDD[String]): Unit = {
    val user = rdd.mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val time = line(0).substring(0, 10)
        list.::=(line(5) + "&" + time, line(11))
      })
      list.iterator
    }).reduceByKey(_ + "," + _).filter(x => {
      val line = x._2.split(",")
      var map: Map[String, String] = Map()
      for (l <- line) {
        if (!map.contains(l)) {
          map += (l -> l)
        }
      }
      map.size > 1
    }).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row._2.split(",")
        var map: Map[String, String] = Map()
        for (l <- line) {
          if (!map.contains(l)) {
            map += (l -> l)
          }
        }
        val arr = new Array[Int](map.size)
        var num = 0
        for (m <- map) {
          arr(num) = m._1.toInt
          num += 1
        }
        scala.util.Sorting.quickSort(arr) //排序

        var str = ""
        for (n <- 0 until arr.length) {
          str += arr(n) + ","
        }
        list.::=(row._1.split("&")(0), str.substring(0, str.length - 1))
      })
      list.iterator
    }).reduceByKey(_ + "#" + _)
      .mapPartitions(x => {
        var list = List[(String)]()
        x.foreach(row => {
          val line = row._2.split("#")
          var map: Map[String, String] = Map()
          for (l <- line) {
            if (!map.contains(l)) {
              map += (l -> l)
            }
          }
          for (m <- map) {
            list.::=(m._1)
          }
        })
        list.iterator
      }).cache()

    user.repartition(1).saveAsTextFile(path)

  }

}
