package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    sevenstone@yeah.net
  * date     2018/5/16 下午6:34
  *
  * 计算点位日均销售额，满足条件的点位
  * 和近十五天无销售记录的点位所属网点
  */
object SalesGoodPoint {

  val conf = new SparkConf().setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val operatePath = "/yst/vem/operate/P/main/*"
    val operateMap = getPointOperateTime(operatePath)
    //广播
    val operateBv = sc.broadcast(operateMap)

    val orderPath = "/yst/vem/sales/order/*"
    val rdd = getOrderPointData(orderPath)

    getPointByMoneyThan60(rdd, operateBv)

    getPointyMoneyThan5(rdd, operateBv)

    val pointPath = "/yst/seven/data/pointData/*"
    salesNullPoint(pointPath, rdd)

  }

  /**
    * 获取网点运营天数,超过15天的网点
    *
    * @param path
    * @return
    */
  def getPointOperateTime(path: String): util.HashMap[String, String] = {
    val operate = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replace("(", "").split(",")
        val pointId = line(0)
        //点位id
        val time = line(2) //运营时间
        hashMap.put(pointId, time)
      })
      hashMap.iterator
    }).filter(x => x._2.toDouble > 15).collect() //选取运营时间大于7天的点位

    val operateMap = new util.HashMap[String, String]
    for (o <- operate) {
      operateMap.put(o._1, o._2)
    }
    println(operateMap)
    operateMap
  }

  /**
    * 获取十五天内的销售数据
    *
    * @param path
    */
  def getOrderPointData(path: String): RDD[(String, Double)] = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    val long = 1000 * 3600 * 24 * 15L
    //15天时间搓
    val order = sc.textFile(path).filter(x => {
      //选取十五天内的销售数据
      val line = x.split(",")
      val time = line(0)
      val money = line(4)
      !"100.0".equals(money) && df.parse(time).getTime > date.getTime - long //过滤十五天以外的数据，过滤活动订单
    }).mapPartitions(x => {
      var list = List[(String, Double)]()
      x.foreach(row => {
        val line = row.split(",")
        val pointId = line(9)
        //点位id
        val netId = line(8)
        //网点id
        val money = line(4).toDouble //订单金额
        list.::=(pointId + "_" + netId, money)
      })
      list.iterator
    }).reduceByKey(_ + _) //聚合所在点位15天内销售金额
      .cache()
    order
  }

  /**
    * 销量好的点位，日均销售额大于六十(15天)
    *
    * @param rdd
    */
  def getPointByMoneyThan60(rdd: RDD[(String, Double)], broadcast: Broadcast[util.HashMap[String, String]]): Unit = {


    val data = rdd.filter(x => {
      //过滤运营时间小于7天的点位
      val map = broadcast.value
      val pointId = x._1.split("_")(0)
      map.containsKey(pointId)
    }).mapPartitions(x => {
      //增加运营天数
      val hashMap = new mutable.HashMap[String, Double]()
      x.foreach(row => {
        val map = broadcast.value
        //获取广播map
        val pointId = row._1.split("_")(0)
        val time = map.get(pointId).toDouble
        //根据点位id获取运营时间
        var avgMoney = 0.0
        if (time >= 15) {
          //获取15天平均销售额，小于15天则取实际天数
          avgMoney = row._2 / 15 / 100
        } else {
          avgMoney = row._2 / time / 100
        }
        hashMap.put(row._1, avgMoney)
      })
      hashMap.iterator
    }).filter(x => x._2 >= 60).mapPartitions(x => {
      //过滤日均销售额小于60的点位。输出网点id
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row._1.split("_")
        list.::=(line(1), 1)
      })
      list.iterator
    }).reduceByKey(_ + _)
      .cache()

    data.foreach(x => println(x._1))
    println("*************************good**********")
  }

  /**
    * 销量差的点位，日均销售额小于五元(15天)
    *
    * @param rdd
    */
  def getPointyMoneyThan5(rdd: RDD[(String, Double)], broadcast: Broadcast[util.HashMap[String, String]]): Unit = {
    val data = rdd.filter(x => {
      //过滤运营时间小于7天的点位
      val map = broadcast.value
      val pointId = x._1.split("_")(0)
      map.containsKey(pointId)
    })
      .mapPartitions(x => {
        //增加运营天数
        val hashMap = new mutable.HashMap[String, Double]()
        x.foreach(row => {
          val map = broadcast.value
          val pointId = row._1.split("_")(0)
          val time = map.get(pointId).toDouble
          var avgMoney = 0.0
          if (time >= 15) {
            //获取15天平均销售额，小于15天则取实际天数
            avgMoney = row._2 / 15 / 100
          } else {
            avgMoney = row._2 / time / 100
          }
          hashMap.put(row._1, avgMoney)
        })
        hashMap.iterator
      }).filter(x => x._2 <= 5).mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row._1.split("_")
        list.::=(line(1), 1)
      })
      list.iterator
    }).reduceByKey(_ + _).cache()

    data.foreach(x => println(x._1))
    println("********bad*****************")
  }

  /**
    * 获取近15天内无销量的网点
    *
    * @param path
    * @param rdd
    */
  def salesNullPoint(path: String, rdd: RDD[(String, Double)]): Unit = {
    val pointRdd = rdd.mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        //获取15天内有销售记录的点位
        val line = row._1.split("_")
        hashMap.put(line(0), line(1))
      })
      hashMap.iterator
    }).collect()

    val pointMap = new util.HashMap[String, String]()
    for (p <- pointRdd) {
      pointMap.put(p._1, p._2)
    }

    val data = sc.textFile(path).filter(x => {
      //获取所有点位信息
      val line = x.split(",")
      !pointMap.containsKey(line(0)) && !"id".equals(line(0)) //过滤抬头，过滤有销售记录的点位
    }).mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(2) //网点id
        list.::=(netId, 1)
      })
      list.iterator
    }).reduceByKey(_ + _).cache()

    data.foreach(x => println(x._1))
    println("*********bad****************")

  }
}
