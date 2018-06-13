package com.seven.spark.rdd

import java.util

import com.seven.spark.sparksql.NetTypeUtils
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
  * date     2018/6/12 下午3:19     
  */
object SeekNetAndPoint {
  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val orderPath = "/yst/vem/sales/order/*"
    val orderRdd = getOrder(orderPath)

    val machinePath = "/yst/sta_vem/vem_machine/*"
    val machineRdd = getMachine(machinePath)

    seekNet(orderRdd,machineRdd)
    println("*********************")
    seekPoint(orderRdd,machineRdd)

  }

  def getOrder(path: String): RDD[String] = {
    val order = sc.textFile(path).cache()
    order
  }

  def getMachine(path: String): RDD[String] = {
    val machine = sc.textFile(path).filter(x => {
      val line = x.split(",")
      "2".equals(line(21)) && !"".equals(line(18)) && !"".equals(line(19)) && "1".equals(line(4)) && "0".equals(line(5))
    }).cache()
    machine
  }

  def seekNet(orderRdd: RDD[String],machineRdd: RDD[String]): Unit = {
    val order = orderRdd.mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(8)
        list.::=(netId, 1)
      })
      list.iterator
    }).reduceByKey(_ + _).collect()

    var  map = Map[String,Int]()

    for (o <- order) {
      map += (o._1 -> o._2)
    }

    val machine = machineRdd.mapPartitions(x =>{
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(18)
        list.::=(netId, 1)
      })
      list.iterator
    }).reduceByKey(_ + _).collect()

    for(m <- machine){
      map += (m._1 -> m._2)
    }

    val netMap = NetTypeUtils.salesNetData(sc)

    for(m <- map){
      if(!netMap.containsKey(m._1)){
        println(m._1)
      }
    }

  }

  def seekPoint(orderRdd: RDD[String],machineRdd: RDD[String]): Unit ={
    val order = orderRdd.mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        val pointId = line(9)
        list.::=(pointId, 1)
      })
      list.iterator
    }).reduceByKey(_ + _).collect()

    var  map = Map[String,Int]()

    for (o <- order) {
      map += (o._1 -> o._2)
    }

    val machine = machineRdd.mapPartitions(x =>{
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        val pointId = line(19)
        list.::=(pointId, 1)
      })
      list.iterator
    }).reduceByKey(_ + _).collect()

    for(m <- machine){
      map += (m._1 -> m._2)
    }

    val pointMap = NetTypeUtils.salesPointData(sc)

    for(m <- map){
      if(!pointMap.containsKey(m._1)){
        println(m._1)
      }
    }
  }

}
