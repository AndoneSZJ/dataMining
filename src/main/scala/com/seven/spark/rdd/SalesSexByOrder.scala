package com.seven.spark.rdd

import java.util
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
  * date     2018/5/14 下午2:37
  * 计算性别购买
  */
object SalesSexByOrder {

  private final val log = LoggerFactory.getLogger(this.getClass)
  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info("job is start ...")
    val stopWatch = new StopWatch()
    stopWatch.start()
    val userPath = "/yst/vem/user/main/*"
    val userMap = salesSexByUser(userPath)
    val userBv = sc.broadcast(userMap)
    val orderPath = "/yst/vem/sales/order/*"
    //    val order = salesUserSexByOrder(orderPath,userBv)
    val order = salesUserSexByPeople(orderPath, userBv)
    order.foreach(println)
    stopWatch.stop()
    log.info("job is success timeout is " + stopWatch.toString)
  }

  /**
    * 计算会员用户性别
    *
    * @param path
    */
  def salesSexByUser(path: String): util.HashMap[String, String] = {
    val user = sc.textFile(path)
      .filter(x => !"".equals(x.split(",")(17))) //过滤性别为空的会员
      .mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replace("(", "").split(",")
        val id = line(0)
        //会员id
        val sex = line(17) //会员性别
        hashMap.put(id, sex)
      })
      hashMap.iterator
    }).collect()
    val userMap = new util.HashMap[String, String]()
    for (u <- user) {
      userMap.put(u._1, u._2)
    }
    userMap
  }

  /**
    * 计算会员销量与性别的关系,按照销量计算
    *
    * @param path
    * @param bv
    */
  def salesUserSexByOrder(path: String, bv: Broadcast[util.HashMap[String, String]]): RDD[(String, Int)] = {
    val order = sc.textFile(path).filter(x => {
      val line = x.split(",")
      val userId = line(5)
      val shopId = line(11)
      //农夫山泉-饮用天然水4L*6桶-整箱                  -> 123
      //万泓WH-169-6T即热式开水机                      -> 228
      //农夫山泉-饮用天然水-4L*2桶                     ->121
      //农夫山泉-饮用天然水量贩装-550ml*12瓶            -> 114
      //农夫山泉-饮用天然水（适合婴幼儿）-1L*12瓶 整箱    -> 151
      //农夫山泉-饮用天然水5L*4桶-整箱                  -> 129
      //农夫山泉-饮用天然水（适合婴幼儿）-1L*8瓶 整箱     -> 219
      //农夫山泉-饮用天然水量贩装-380ml*12瓶            -> 108
      //农夫山泉-饮用天然矿泉水-535ml*6瓶               -> 157
      //农夫山泉-饮用天然水-5L*2桶                     -> 122
      val userMap = bv.value
      val sex = userMap.get(userId)
      //过滤不是会员的订单和水订单
      !"123".equals(shopId) && !"228".equals(shopId) && !"121".equals(shopId) && !"114".equals(shopId) && !"151".equals(shopId) && !"129".equals(shopId) && !"219".equals(shopId) && !"108".equals(shopId) && !"157".equals(shopId) && !"122".equals(shopId) && !(sex == null)
    }).mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        val userId = line(5)
        val shopName = line(12)
        val shopId = line(11)
        val userMap = bv.value
        val sex = userMap.get(userId)
        //记录名称
        var sign = ""
        if (shopName.contains("NFC")) {
          sign = "NFC"
        } else if (shopName.contains("东方树叶")) {
          sign = "东方树叶"
        } else if (shopName.contains("水溶C100")) {
          sign = "水溶C100"
        } else if (shopName.contains("清嘴")) {
          sign = "清嘴"
        } else if (shopName.contains("尖叫")) {
          sign = "尖叫"
        } else if (shopName.contains("茶π")) {
          sign = "茶π"
        } else if (shopName.contains("维他命")) {
          sign = "维他命"
        } else if (shopName.contains("牛肉棒")) {
          sign = "牛肉棒"
        } else {
          sign = "OTHER"
        }
        //          if("1".equals(sex)){
        //            list .::= ("MAN_"+shopId,1)
        //          }else{
        //            list .::= ("WOMAN_"+shopId,1)
        //          }
        if ("1".equals(sex)) {
          list.::=("MAN_" + sign, 1)
        } else {
          list.::=("WOMAN_" + sign, 1)
        }
      })
      list.iterator
    }).reduceByKey(_ + _).cache()
    order
  }


  /**
    * 计算会员销量与性别的关系,按人数计算
    *
    * @param path
    * @param bv
    */
  def salesUserSexByPeople(path: String, bv: Broadcast[util.HashMap[String, String]]): RDD[(String, Int)] = {
    val order = sc.textFile(path).filter(x => {
      val line = x.split(",")
      val userId = line(5)
      val shopId = line(11)
      //农夫山泉-饮用天然水4L*6桶-整箱                  -> 123
      //万泓WH-169-6T即热式开水机                      -> 228
      //农夫山泉-饮用天然水-4L*2桶                     ->121
      //农夫山泉-饮用天然水量贩装-550ml*12瓶            -> 114
      //农夫山泉-饮用天然水（适合婴幼儿）-1L*12瓶 整箱    -> 151
      //农夫山泉-饮用天然水5L*4桶-整箱                  -> 129
      //农夫山泉-饮用天然水（适合婴幼儿）-1L*8瓶 整箱     -> 219
      //农夫山泉-饮用天然水量贩装-380ml*12瓶            -> 108
      //农夫山泉-饮用天然矿泉水-535ml*6瓶               -> 157
      //农夫山泉-饮用天然水-5L*2桶                     -> 122
      val userMap = bv.value
      val sex = userMap.get(userId)
      //过滤不是会员的订单和水订单
      !"123".equals(shopId) && !"228".equals(shopId) && !"121".equals(shopId) && !"114".equals(shopId) && !"151".equals(shopId) && !"129".equals(shopId) && !"219".equals(shopId) && !"108".equals(shopId) && !"157".equals(shopId) && !"122".equals(shopId) && !(sex == null)
    }).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val userId = line(5)
        val shopId = line(11)
        list.::=(userId, shopId)
      })
      list.iterator
    }).reduceByKey(_ + "," + _)
      .mapPartitions(x => {
        var list = List[(String, String)]()
        x.foreach(row => {
          val line = row._2.split(",")
          var map: Map[String, String] = Map()
          for (l <- line) {
            if (!map.contains(l)) {
              //去重
              map += (l -> l)
            }
          }
          for (m <- map) {
            list.::=(row._1, m._1)
          }
        })
        list.iterator
      }).mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val userId = row._1
        val shopId = row._2
        val userMap = bv.value
        val sex = userMap.get(userId)
        //判断男女
        if ("1".equals(sex)) {
          list.::=("MAN_" + shopId, 1)
        } else {
          list.::=("WOMAN_" + shopId, 1)
        }
      })
      list.iterator
    }).reduceByKey(_ + _).cache()
    order
  }


}
