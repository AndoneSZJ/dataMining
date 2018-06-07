package com.seven.spark.rdd

import org.apache.spark.SparkContext

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
  * date     2018/5/10 上午10:37
  */
object TestMap {
  def main(args: Array[String]): Unit = {
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/disp/point/static/main/"
    //val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //val data = sc.textFile(path)
    //val a = sc.parallelize(1 to 9, 3)

    //println(data.map(x => toMap(x)).count())

    //    println(data.mapPartitions(x => toMapPatitions(x)).count())
    //
    //    println(a.map(x => toMap(x)).count())
    //    println(a.mapPartitions(x => toMapPatitionsa(x)).count())
    //    println(a.map(x => toMap(x)).collect().mkString)
    //    println(a.mapPartitions(x => toMapPatitionsa(x)).collect().mkString)
    //flatmapTransformation(sc)

    var map: Map[String, Int] = Map()

    map += ("aaa8" -> 2)
    map += ("aaa1" -> 3)
    map += ("aaa2" -> 4)
    map += ("aaa3" -> 5)
    map += ("aaa4" -> 6)
    map += ("aaa7" -> 7)

    for (m <- map) {
      println(m._1 + ":" + m._2)
    }

    println(map.max._2)

    println(map.get("aaa7").getOrElse(8))

    val arr: Array[Int] = Array(2, 4, 6, 1)
    println(arr.max + ":" + arr.min)

    var sssss = 1
    while (sssss < 6) {
      println("#########")
      sssss += 1
    }

    val a: Double = 3.toDouble / 2
    println(a)

    val aaa = "123456"
    println(aaa.substring(0, aaa.length - 1))

  }

  def toMap(row: String): (String, String) = {
    val line = row.split(",")
    (line(0), row)
  }

  def toMap(row: Int): (Int, Int) = {
    (row, row * row)
  }

  def toMapPatitions(iterator: Iterator[String]): Iterator[(String, String)] = {
    var res = List[(String, String)]()
    while (iterator.hasNext) {
      val line = iterator.next
      res.::=(line.split(",")(0), line)
    }
    res.iterator
  }

  def toMapPatitionsa(iterator: Iterator[Int]): Iterator[(Int, Int)] = {
    val hashMap = new mutable.HashMap[Int, Int]()
    while (iterator.hasNext) {
      val line = iterator.next
      hashMap.put(line, line * line)
    }
    hashMap.iterator
  }

  def flatmapTransformation(sc: SparkContext): Unit = {
    val bigData = Array("scala", "spark", "java_Hadoop", "java_tachyon")
    val bigDataString = sc.parallelize(bigData)
    val words = bigDataString.flatMap(line => line.split("_"))
    words.collect.foreach(println)
  }

}
