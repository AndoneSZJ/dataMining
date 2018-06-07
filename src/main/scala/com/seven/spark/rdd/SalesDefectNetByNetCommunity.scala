package com.seven.spark.rdd

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/6/6 下午2:59
  *
  * 计算net_community中缺失的网点
  */
object SalesDefectNetByNetCommunity {

  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info(this.getClass.getSimpleName+" is start . . .")
    val netCommunityPath = "/yst/sta_vem/net_community/*"
    val netCommunityMap = getNetCommunityData(netCommunityPath)
    val netCommunityBv = sc.broadcast(netCommunityMap)

    val netTypePath = "/yst/sta_vem/vem_nettype/*"
    val netTypeMap = getNetTypeData(netTypePath)
    val netTypeBv = sc.broadcast(netTypeMap)

    val path = "/yst/vem/sales/order/*"

    salesDefectNetData(path,netCommunityBv,netTypeBv)

    log.info(this.getClass.getSimpleName+" is success . . .")
  }

  /**
    * 获取net_community数据
    * @param path
    * @return
    */
  def getNetCommunityData(path:String): util.HashMap[String,String] ={
    val net = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      "0".equals(line(24))//保留正常网点
    }).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val line = row.split(",")
        val netId = line(0)
        hashMap.put(netId,row)
      })
      hashMap.iterator
    }).collect()

    val map = new util.HashMap[String,String]()
    for(n <- net){
      map.put(n._1,n._2)
    }
    map
  }

  /**
    * 获取nettype中对映关系
    * @param path
    * @return
    */
  def getNetTypeData(path:String): util.HashMap[String,String] ={
    val net = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      "net".equals(line(8))
    }).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val line = row.split(",")
        hashMap.put(line(0),line(20))
      })
      hashMap.iterator
    }).collect()
    val map = new util.HashMap[String,String]()
    for(n <- net){
      map.put(n._1,n._2)
    }
    map
  }

  /**
    * 输出找不到的网点信息
    * @param path
    * @param communityBy
    * @param netTypeBv
    */
  def salesDefectNetData(path:String,communityBy:Broadcast[util.HashMap[String,String]],netTypeBv:Broadcast[util.HashMap[String,String]]): Unit ={
    val data = sc.textFile(path)
//      .filter(x =>{
//      val line = x.split(",")
//      "2".equals(line(21)) && "1".equals(line(4)) && "0".equals(line(5))
//    })
      .mapPartitions(x =>{
      var list =List[(String,String)]()
      x.foreach(row =>{
        val line = row.split(",")
        list .::= (line(8),"")
      })
      list.iterator
    }).reduceByKey(_+_)
      .mapPartitions(x =>{
      var list =List[(String,String)]()
      x.foreach(row =>{
        val netType = netTypeBv.value
        list .::= (netType.get(row._1),row._1)
      })
      list.iterator
    })
      .filter(x =>{
      val netCommunity = communityBy.value
      !netCommunity.containsKey(x._1)
    })
      .cache()

    data.foreach(println)
  }

}
