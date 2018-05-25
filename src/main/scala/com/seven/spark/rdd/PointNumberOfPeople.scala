package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/** *
  * author seven
  * time   2018-04-19
  * 统计同一点位每七日新用户数，之前购买过视为老用户
  */
object PointNumberOfPeople {

  private final val log = LoggerFactory.getLogger(PointNumberOfPeople.getClass)

  def main(args: Array[String]): Unit = {

    log.info("job " + this.getClass.getSimpleName + " is start . . . ")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    log.info("start . . .")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    log.info("jobStartTime is " + format.format(date.getTime))
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()


    //小区数据路径
    val pathCommunity = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/net_community/*"

    //获取小区map
    //val communityMap = salesByCommunity(sc,pathCommunity)

    //广播小区关系
    //val communityMapBv:Broadcast[util.HashMap[String, String]] = sc.broadcast(communityMap)

    //网点数据路径
    val pathNettype = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_nettype/*"

    //获取网点map
    val nettypeMap = salesByNettype(sc, pathNettype)

    //广播网点关系
    val nettypeBv = sc.broadcast(nettypeMap)

    //订单数据路径
    val orderPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order/"

    val data = salesPointNum(sc, orderPath, nettypeBv)

    data.foreach(x => println(x._1 + "," + x._2))
    data.map(x => (x._2)).repartition(1).saveAsTextFile("/Users/seven/data/point/")


    stopWatch.stop()
    sc.stop()
    log.info("job time consuming is " + stopWatch.toString)

    log.info("jobEndTime is " + format.format(date.getTime))
    log.info("job is success")
    log.info("end . . .")
  }

  /**
    * 计算省、市、区、小区名称
    *
    * @param path
    */
  def salesByCommunity(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesCommunity is start . . .")
    val community = sc.textFile(path)
      .map(x => communityToMap(x.toString)).collect()

    val communityMap = new util.HashMap[String, String]
    for (c <- community) {
      communityMap.put(c._1, c._2)
    }
    log.info("salesCommunity is end . . .")
    communityMap
  }

  /**
    * 计算小区信息map
    *
    * @param row
    * @return
    */
  def communityToMap(row: String): (String, String) = {
    val line = row.toString.split(",")
    val communityId = line(0)
    //小区id
    val communityName = line(1)
    //小区名称
    val city = line(4) + "_" + line(5) //省区_市区
    (communityId, communityName + "&" + city)
  }

  /**
    * 计算机器和小区点位的关系
    *
    * @param sc
    * @param path
    */
  def salesByNettype(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesNettype is start . . .")
    val nettype = sc.textFile(path)
      .map(x => nettypeToMap(x.toString)).collect()

    val nettypeMap = new util.HashMap[String, String]
    for (n <- nettype) {
      nettypeMap.put(n._1, n._2)
    }
    log.info("salesNettype is end . . .")
    nettypeMap
  }

  /**
    * 计算小区关系map
    *
    * @param row
    * @return
    */
  def nettypeToMap(row: String): (String, String) = {
    val line = row.toString.split(",")
    val nettypeId = line(0)
    //网点id
    val createtime = line(4)
    //创建时间
    val name = line(1)
    //名称
    val city = line(11) + "_" + line(12) //省区_市区
    (nettypeId, createtime + "&" + city + "&" + name)
  }

  /**
    * 计算点位信息，排除活动用户
    *
    * @param sc
    * @param path
    * @param broadcast
    * @return
    */
  def salesPointNum(sc: SparkContext, path: String, broadcast: Broadcast[util.HashMap[String, String]]): RDD[(String, String)] = {

    val pointNum = sc.textFile(path).map(x => onePointMap(x.toString, broadcast))
      .filter(x => !"100.0".equals(x._1.toString)) //排除活动销量纪录
      .map(x => (x._2.toString.split(",")(5), x._2 + "," + x._1)) //将点位id作为聚合的条件
      .reduceByKey(_ + "&" + _).map(x => twoPointMap(x._1.toString, x._2.toString)).cache()
    pointNum
  }

  /**
    * 第一次map运算
    *
    * @param row
    * @param nettypeBv
    * @return
    */
  def onePointMap(row: String, nettypeBv: Broadcast[util.HashMap[String, String]]): (String, String) = {
    val line = row.split(",")
    val foundTime = line(0)
    //创建时间
    val amountOfPayment = line(4)
    //支付金额
    val paymentAccount = line(5)
    //支付账号
    val paymentTime = line(6)
    //支付时间
    val netId = line(8)
    //网点id
    val pointId = line(9)
    //点位id
    val netMap = nettypeBv.value
    val point = netMap.get(pointId)
    val net = netMap.get(netId)
    val ctrateTime = net.split("&")(0)
    //创建时间
    val communityName = point.split("&")(2)
    //小区名称
    val city = net.split("&")(1) //省市
    (amountOfPayment, foundTime + "," + paymentAccount + "," + paymentTime + "," + communityName + "," + city + "," + pointId + "," + ctrateTime)
  }

  /**
    * 第二次map计算
    *
    * @param row_1
    * @param row_2
    */
  def twoPointMap(row_1: String, row_2: String): (String, String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    val long = 1000 * 3600 * 24 * 7L
    //七天时间
    val lines = row_2.split("&")
    val ll = lines(0)
    val city = ll.split(",")(4)
    //城市
    val createTime = ll.split(",")(6)
    //创建时间
    val communityName = ll.split(",")(3)
    //小区名称
    var time = format.parse(createTime).getTime
    //获取创建时间搓
    var map: Map[String, Long] = Map()
    for (l <- lines) {
      val line = l.split(",")
      if (!map.contains(line(1))) {
        //去重，多条记录只算一次
        map += (line(1) -> format.parse(line(2)).getTime)
      }
    }
    var number = ""
    while (time < date.getTime) {
      var num = 0
      val timeNum = time + long //获取七天内的时间搓
      for (k <- map.keySet) {
        if (map(k) < timeNum && map(k) > time) {
          //获取在七天内的销量
          num += 1
        }
      }
      number += num + ","
      time = timeNum
    }
    var n = 0
    for (k <- map.keySet) {
      //获取最后不足七天的销量
      if (map(k) < date.getTime && map(k) > time) {
        n += 1
      }
    }
    number += n //获取销量数据
    (row_1, city + "," + communityName + "," + number)
  }
}
