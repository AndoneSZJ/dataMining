package com.seven.spark.rdd

import java.util

import com.seven.spark.utils.Utils
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    sevenstone@yeah.net
  * date     2018/5/22 上午10:09     
  */
object AvgMouthByZfj {

  private final val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if (args.length == 0 || args == null) {
      conf.setMaster("local[2]")
    }
    val sc = new SparkContext(conf)
    log.info("job is start . . .")
    val stopwatch = new StopWatch()
    stopwatch.start()
    val zfjPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*"
    //获取自贩机基础信息
    val rdd = getBasicDataByZfj(zfjPath, sc)
    //获取渠道id->渠道名称
    val channelMap = getChannelNameByZfj(rdd)
    //广播
    val channelBv = sc.broadcast(channelMap)
    //获取大区id->大区名称
    val areaMap = getAreaNameByZfj(rdd)
    //广播
    val areaBv = sc.broadcast(areaMap)
    val abnormalPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/result/abnormalVM/*"
    //获取异常自贩机信息
    val abnormalMap = getAbnormalDataByZfj(abnormalPath, sc)
    //广播
    val abnormalBv = sc.broadcast(abnormalMap)
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/DW_ZFJ_RLB_ALL/*"
    getAvgMouthDataByZfj(path, sc, abnormalBv, channelBv, areaBv)
    stopwatch.stop()
    log.info("job is success . . .  spend time is " + stopwatch.toString)

  }

  /**
    * 获取自贩机基础信息
    *
    * @param path
    * @param sc
    * @return
    */
  def getBasicDataByZfj(path: String, sc: SparkContext): RDD[String] = {
    val data = sc.textFile(path).filter(x => {
      val line = x.split(",")
      line.size > 62 && "1".equals(line(4))
    }).cache()
    data
  }

  /**
    * 获取自贩机渠道关系
    *
    * @param rdd
    * @return
    */
  def getChannelNameByZfj(rdd: RDD[String]): util.HashMap[String, String] = {
    val data = rdd.mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.split(",")
        hashMap.put(line(28), line(30)) //渠道id->渠道名称
      })
      hashMap.iterator
    }) collect()

    val channelMap = new util.HashMap[String, String]()
    for (d <- data) {
      channelMap.put(d._1, d._2)
    }
    channelMap
  }

  /**
    * 获取自贩机大区关系
    *
    * @param rdd
    * @return
    */
  def getAreaNameByZfj(rdd: RDD[String]): util.HashMap[String, String] = {
    val data = rdd.mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.split(",")
        hashMap.put(line(51), line(52)) //大区id->大区名称
      })
      hashMap.iterator
    }) collect()

    val areaMap = new util.HashMap[String, String]()
    for (d <- data) {
      areaMap.put(d._1, d._2)
    }
    areaMap
  }

  /**
    * 获取异常刷单网点信息
    *
    * @param path
    * @return
    */
  def getAbnormalDataByZfj(path: String, sc: SparkContext): util.HashMap[String, Int] = {
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
    abnormalMap
  }


  /**
    * 获取平均值
    *
    * @param path
    * @param sc
    * @param abnormalBv
    * @param channelBv
    * @param areaBv
    */
  def getAvgMouthDataByZfj(path: String, sc: SparkContext,
                           abnormalBv: Broadcast[util.HashMap[String, Int]],
                           channelBv: Broadcast[util.HashMap[String, String]],
                           areaBv: Broadcast[util.HashMap[String, String]]): Unit = {
    val data = sc.textFile(path).filter(x => {
      val line = x.replaceAll("[()]", "").split(",")
      val abnormalMap = abnormalBv.value
      val vmId = line(1)
      //自贩机id
      val netId = line(2)
      //网点id
      val channelId = line(3)
      //渠道id
      val areaId = line(4)
      //大区id
      val mouth = line(5)
      //月份
      val money = line(6) //金额
      !abnormalMap.containsKey(vmId) && !"".equals(netId) && !"".equals(channelId) && !"".equals(areaId) && !"".equals(mouth) && !"".equals(money)
    }).map(x => {
      val line = x.replaceAll("[()]", "").split(",")
      val vmId = line(1) //自贩机id
      (vmId, x.replaceAll("[()]", ""))
    }).reduceByKey(_ + "@" + _).map(x => {
      //根据自贩机id聚合,计算自贩机月销量
      val lines = x._2.split("@")
      var avgMoney = 0.0
      val channelId = lines(0).split(",")(3) //渠道id
      for (l <- lines) {
        val line = l.split(",")
        avgMoney += line(6).toDouble
      }
      avgMoney = avgMoney / 12
      (x._1 + "," + channelId, avgMoney + "," + lines.size)
    }).filter(x => (x._2.split(",")(1)).toInt > 7).map(x => {
      (x._1.split(",")(1), x._2.split(",")(0))
    }).reduceByKey(_ + "," + _).filter(x => x._2.split(",").size > 10).map(x => {
      //根据自贩机渠道id聚合，计算该渠道下自贩机的数据
      val channelMap = channelBv.value
      val lines = x._2.split(",")
      val size = lines.size
      val arr = new Array[Double](size)
      for (d <- 0 until size) {
        arr(d) = lines(d).toDouble
      }
      scala.util.Sorting.quickSort(arr)
      //排序
      val lower = arr(1).formatted("%.2f")
      //保留两位小数
      val upper = arr(size - 2).formatted("%.2f")
      val middle = arr(size / 2).formatted("%.2f")
      val q3 = arr(size * 4 / 5).formatted("%.2f")
      val q1 = arr(size * 1 / 5).formatted("%.2f")
      val channelName = channelMap.get(x._1)
      (channelName + "," + upper + "," + q3 + "," + middle + "," + q1 + "," + lower)
    }).cache()
    val savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/ZFJ_AVG_DATA/"
    Utils.saveHdfs(data, sc, savePath, 1)
    //    data.repartition(1).saveAsTextFile(savePath)

  }
}
