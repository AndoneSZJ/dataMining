package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

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
  * date     2018/4/18 上午9:37
  *
  * 计算同一账号购买记录时间间隔
  * 活动排除
  */
object SalesTimeInterval {
  private final val log = LoggerFactory.getLogger(SalesTimeInterval.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    log.info("start . . .")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    log.info("jobStartTime is " + format.format(date.getTime))
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()

    //小区数据路径
    val pathCommunity = "/yst/sta_vem/net_community/*"

    //获取小区map
    val communityMap = salesCommunity(sc, pathCommunity)

    //广播小区关系
    val communityMapBv: Broadcast[util.HashMap[String, String]] = sc.broadcast(communityMap)

    //网点数据路径
    val pathNettype = "/yst/sta_vem/vem_nettype/*"

    //获取网点map
    val nettypeMap = salesNettype(sc, pathNettype, communityMapBv)

    //广播网点关系
    val nettypeBv = sc.broadcast(nettypeMap)

    //自贩机数据路径
    //val pathMachine = "/yst/sta_vem/vem_machine/*"

    //获取自贩机map
    //val machineMap = salesMachine(sc,pathMachine)

    //广播自贩机关系
    //val machineBv = sc.broadcast(machineMap)

    //订单数据路径
    //val orderPath = "/yst/sta_vem/vem_order/*"
    val orderPath = "/yst/vem/sales/order/"

    //获取结果
    val data = salesOrder(sc, orderPath, nettypeBv)

    data.map(x => (x._2.toString.substring(0, x._2.toString.length - 1))).repartition(1).saveAsTextFile("/yst/seven/data/order/")

    stopWatch.stop()
    sc.stop()
    log.info("job time consuming is " + stopWatch.toString)

    log.info("jobEndTime is " + format.format(date.getTime))
    log.info("job is success")
    log.info("end . . .")

  }


  /**
    * 计算两个时间差   单位：天
    *
    * @param startTime
    * @param endTIme
    * @return
    */
  def timeDifference(startTime: String, endTIme: String): String = {
    println("startTime:" + startTime + "," + "endTime:" + endTIme)
    if ("".contains(startTime) || "".contains(endTIme)) return ""
    //开始天数
    val stime = startTime.trim.split(" ")(0) + " 00:00:00"
    //结束天数
    val etime = endTIme.trim.split(" ")(0) + " 00:00:00"

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //计算日期
    val num = (df.parse(etime).getTime - df.parse(stime).getTime) / 86400000
    num.toString
  }

  /**
    * 计算两个时间搓差  单位：天
    *
    * @param startTime
    * @param endTime
    * @return
    */
  def timeDifference(startTime: Long, endTime: Long): String = {
    val num = (endTime - startTime) / 86400000
    num.toString
  }

  /**
    * 计算省、市、区、小区名称
    *
    * @param path
    */
  def salesCommunity(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesCommunity is start . . .")
    val community = sc.textFile(path)
      .mapPartitions(x => {
        val hashMap = new mutable.HashMap[String, String]()
        x.foreach(row => {
          val line = row.toString.split(",")
          val communityId = line(0)
          //小区id
          val communityName = line(1)
          //小区名称
          val city = line(4) + "_" + line(5) //省区_市区
          hashMap.put(communityId, communityName + "&" + city)
        })
        hashMap.iterator
      }).collect()

    val communityMap = new util.HashMap[String, String]
    for (c <- community) {
      communityMap.put(c._1, c._2)
    }
    log.info("salesCommunity is end . . .")
    communityMap
  }


  /**
    * 计算机器和小区的关系
    *
    * @param sc
    * @param path
    * @param bv 计算小区结果
    */
  def salesNettype(sc: SparkContext, path: String, bv: Broadcast[util.HashMap[String, String]]): util.HashMap[String, String] = {
    log.info("salesNettype is start . . .")
    val nettype = sc.textFile(path)
      .filter(x => {
        val line = x.toString.split(",")
        if ("net".equals(line(8)) && "0".equals(line(7)) && "1".equals(line(19))) {
          true
        } else {
          false
        }
      }).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.toString.split(",")
        val nettypeId = line(0)
        //网点id
        val communityId = line(20)
        //小区id
        val map = bv.value
        //小区关系
        val communityName = map.get(communityId).toString //获取小区信息
        hashMap.put(nettypeId, communityName)
      })
      hashMap.iterator
    }).collect()
    val nettypeMap = new util.HashMap[String, String]
    for (n <- nettype) {
      nettypeMap.put(n._1, n._2)
    }
    log.info("salesNettype is end . . .")
    nettypeMap
  }

  /**
    * 获取自贩机编号关系
    *
    * @param sc
    * @param path
    * @return
    */
  def salesMachine(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesMachine is start . . .")
    val machine = sc.textFile(path).filter(x => ("2".equals(x.toString.split(",")(21))))
      .map(x => (x.toString.split(",")(0))).collect() //获取自贩机为格子机的编号

    val machineMap = new util.HashMap[String, String]()
    for (m <- machine) {
      machineMap.put(m, m)
    }
    log.info("salesMachine is end . . .")
    machineMap
  }

  /** *
    * 计算同一账户购买时间间隔
    *
    * @param sc
    * @param path
    * @param nettypeBv
    */
  def salesOrder(sc: SparkContext, path: String, nettypeBv: Broadcast[util.HashMap[String, String]]
                 //                 ,machineBv:Broadcast[util.HashMap[String, String]]
                ): RDD[(String, String)] = {
    val order = sc.textFile(path)
      .map(x => oneMap(x.toString, nettypeBv)) //第一次map运算
      .filter(x => (!x._1.equals("100.0"))) //过滤活动记录
      .map(x => (x._2.toString.split(",")(1), x._2 + "," + x._1))
      .reduceByKey(_ + "&" + _).map(x => twoMap(x._1.toString, x._2.toString))
      .filter(x => (!"1".equals(x._1))).cache() //过滤只够买一次或只在同一天购买过多次的记录
    order
  }

  /**
    * 第一次map运算
    *
    * @param row
    * @param nettypeBv
    * @return
    */
  def oneMap(row: String, nettypeBv: Broadcast[util.HashMap[String, String]]): (String, String) = {
    val line = row.split(",")
    val foundTime = line(0)
    //创建时间
    val amountOfPayment = line(4)
    //支付金额
    val paymentAccount = line(5)
    //支付账号
    val paymentTime = line(6)
    //支付时间
    val nettypeId = line(8)
    //网点id
    val map = nettypeBv.value
    val net = map.get(nettypeId)
    val communityName = net.split("&")(0)
    //小区名称
    val city = net.split("&")(1) //省市
    (amountOfPayment, foundTime + "," + paymentAccount + "," + paymentTime + "," + communityName + "," + city)
  }

  /**
    * 第二次map运算
    *
    * @param row_1
    * @param row_2
    * @return
    */
  def twoMap(row_1: String, row_2: String): (String, String) = {
    val lines = row_2.split("&")
    val line = lines(0).split(",")
    val communityName = line(3)
    //小区名称
    val city = line(4)
    //城市
    var number = 1
    //购买次数
    val array = new Array[Long](lines.length)
    //创建用来排序时间的数组
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //遍历
    for (s <- 0 until lines.length) {
      val ss = lines(s).split(",")(2).split(" ")(0) //获取时间yyyy-mm-dd
      array(s) = df.parse(ss + " 00:00:00").getTime //获取时间搓，存放至数组中
    }
    //数组排序
    scala.util.Sorting.quickSort(array)
    if (array.length < 2) {
      //只有一条记录数据
      (number.toString, row_1 + "," + city + "," + communityName)
    } else {
      var timeByDifference = ""
      for (a <- 1 until array.length) {
        val ss = timeDifference(array(a - 1), array(a))
        //        println(ss)
        if (!"0".equals(ss)) {
          //多条记录且不在同一天
          number += 1
          timeByDifference += ss + "," //时间间隔天数
        }
      }
      (number.toString, row_1 + "," + city + "," + communityName + "," + timeByDifference)
    }
  }
}
