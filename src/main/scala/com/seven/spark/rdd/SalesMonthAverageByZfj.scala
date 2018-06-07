package com.seven.spark.rdd

import java.util

import com.seven.spark.hdfs.Utils
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
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
  * date     2018/5/18 上午11:31
  *
  * 计算过去一年内渠道月销售金额
  */
object SalesMonthAverageByZfj {
  private final val log = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    if (args == null || args.length == 0) {
      conf.setMaster("local[2]")
    }
    val sc = new SparkContext(conf)

    log.info("parameter number is " + args.length)
    log.info("job is start ...")
    val stopWatch = new StopWatch()
    stopWatch.start()

    val abnormalPath = "/yst/zfj/result/abnormalVM/*"
    val abnormalMap = getSalesAbnormalByZfj(abnormalPath, sc)
    val abnormalBv = sc.broadcast(abnormalMap)


    val orderPath = "/yst/zfj/find/DW_ZFJ_RLB_ALL/*"
    salesAverageMonthInNetByZfj(orderPath, abnormalBv, sc)

    stopWatch.stop()
    log.info("job is success timeout is " + stopWatch.toString)


  }


  /**
    * 获取异常刷单网点信息
    *
    * @param path
    * @return
    */
  def getSalesAbnormalByZfj(path: String, sc: SparkContext): util.HashMap[String, Int] = {
    log.info("getSalesAbnormalByZfj is start . . . ")
    val abnormal = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, Int]()
      x.foreach(row => {
        val line = row.split(",")
        val vmId = line(0)
        hashMap.put(vmId, 1)
      })
      hashMap.iterator
    }).collect()

    val abnormalMap = new util.HashMap[String, Int]()
    for (a <- abnormal) {
      abnormalMap.put(a._1, a._2)
    }
    log.info("getSalesAbnormalByZfj is success . . . ")
    abnormalMap
  }


  /**
    * 计算 大区-渠道-月份-台销
    *
    * @param path
    * @param broadcast
    */
  def salesAverageMonthInNetByZfj(path: String, broadcast: Broadcast[util.HashMap[String, Int]], sc: SparkContext): Unit = {
    log.info("order data sales is start . . . ")
    val data = sc.textFile(path).filter(x => {
      val line = x.split(",")
      //组织id，自贩机id，渠道id，大区id，订单交易时间，订单交易金额，1，支付方式，交易账号
      val vmId = line(1)
      //自贩机id
      val orderMoney = line(6)
      //订单交易金额
      val channelId = line(3)
      //渠道id
      val areaId = line(4)
      //大区id
      val abnormalMap = broadcast.value
      //过滤异常渠道id和大区id
      channelId.length < 20 && areaId.length < 20 && !"".equals(orderMoney) && !abnormalMap.containsKey(vmId) //过滤异常自贩机
    }).mapPartitions(x => {
      //分类  大区-渠道-月份
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val orderTime = line(5)
        //订单交易时间
        val time = orderTime.substring(0, 7)
        //获取时间月份
        val orderMoney = line(6)
        //订单交易金额
        val channelId = line(3)
        //渠道id
        val vmId = line(1)
        //自贩机id
        val areaId = line(4) //大区id
        list.::=(areaId + "," + channelId + "," + time, vmId + "," + orderMoney + ",seven")
      })
      list.iterator
    }).reduceByKey(_ + "@" + _).mapPartitions(x => {
      //获取台销
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val lines = row._2.split("@")
        println(lines)
        val map = new mutable.HashMap[String, Boolean]()
        //存储自贩机信息
        var moneys = 0.0 //月销售总金额
        for (l <- lines) {
          //遍历，获取订单下包含的自贩机
          val line = l.split(",")
          if (!map.contains(line(0))) {
            //不存在则加入
            map.put(line(0), true)
          }
          moneys += line(1).toDouble
        }
        val avgMoney: Double = moneys / map.size //平均销售金额
        hashMap.put(row._1, avgMoney.toString)
      })
      hashMap.iterator
    }).mapPartitions(x => {
      var list = List[(String)]()
      x.foreach(row => {
        list.::=(row._1 + "," + row._2)
      })
      list.iterator
    }).cache()
    log.info("order data sales is success . . . ")
    Utils.saveHdfs(data, sc, "/yst/vem/sales/seven/SalesMonthAverageByZfj/", 0)
    //data.repartition(1).saveAsTextFile("/yst/vem/sales/seven/SalesMonthAverageByZfj/")
    log.info("data write hdfs is success . . . ")
  }
}
