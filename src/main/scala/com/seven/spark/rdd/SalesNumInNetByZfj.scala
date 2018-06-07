package com.seven.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}


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
  * date     2018/5/18 上午9:31
  */
object SalesNumInNetByZfj {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val path = "/yst/sta_ods/dw_dim_zfj/*"
    salesNumByZfj(path)
  }

  /**
    * 计算
    *
    * @param path
    */
  def salesNumByZfj(path: String): Unit = {
    val data = sc.textFile(path).filter(x => {
      val line = x.split(",") //4->售货机状态代码
      line.length > 62 && "1".equals(line(4)) //过滤数据
    }).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(34)
        //网点id
        val netName = line(35).replace("@", "")
        //网点名称
        val vmId = line(1) //自贩机id
        if ("".equals(netId)) {
          println(vmId)
        }
        list.::=(netId, vmId + "," + netName + ",seven")
      })
      list.iterator
    }).reduceByKey(_ + "@" + _).mapPartitions(x => {
      var list = List[(String)]()
      x.foreach(row => {
        val lines = row._2.split("@")
        val netName = lines(0).split(",")(1)
        var map: Map[String, Boolean] = Map()
        for (l <- lines) {
          val line = l.split(",")
          if (!map.contains(line(0))) {
            map += (line(0) -> true)
          }
        }
        list.::=(row._1 + "," + netName + "," + map.size)
      })
      list.iterator
    }).cache()

    data.repartition(1).saveAsTextFile("/yst/seven/data/zfiNum/zfj/")
  }

}
