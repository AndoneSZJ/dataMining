package com.seven.spark.sparksql

import java.util

import org.apache.spark.{SparkConf, SparkContext}


/**
  * author seven
  * time   2018-05-02
  * 处理点位和网点信息工具类
  */
object NetTypeUtils {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    salesPointData(sc)
  }

  /**
    * 计算点位信息，返回点位map，key为点位id
    * @param sc
    * @return
    */
  def salesPointData(sc:SparkContext): util.HashMap[String,String] ={
    val point = sc.textFile("/Users/seven/data/pointData/*")
        .filter(x => !"id".equals(x.toString.split(",")(0)))//去列头
        .mapPartitions(x =>{
          var list = List[(String,String)]()
          x.foreach(row =>{
            val line = row.toString.split(",")
            list .::= (line(0),row+",seven")
          })
          list.iterator
        }).collect()

    val pointMap = new util.HashMap[String,String]()

    for(p <- point){
      pointMap.put(p._1,p._2)
    }
    pointMap
  }

  /**
    * 计算网点信息，返回网点map，key为网点id
    * @param sc
    * @return
    */
  def salesNetData(sc:SparkContext): util.HashMap[String,String] ={
    val net = sc.textFile("/Users/seven/data/netData/*")
      .filter(x => !"id".equals(x.toString.split(",")(0)))//去除列头
      .mapPartitions(x =>{
        var list = List[(String,String)]()
        x.foreach(row =>{
          val line = row.toString.split(",")
          list .::= (line(0),row+",seven")
        })
        list.iterator
      }).collect()
    val netMap = new util.HashMap[String,String]()
    for(n <- net){
      netMap.put(n._1,n._2)
    }
    netMap
  }
}
