package com.seven.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/18 上午10:17     
  */
object SalesNumInChannelByZfj {
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*"
    salesNumInChannelByZfj(path)
  }

  /**
    * 计算
    * @param path
    */
  def salesNumInChannelByZfj(path:String): Unit ={
    val data = sc.textFile(path).filter(x =>{
      val line = x.split(",")//4->售货机状态代码
      line.length > 62 && "1".equals(line(4))//过滤数据
    }).mapPartitions(x =>{
      var list =List[(String,String)]()
      x.foreach(row =>{
        val line = row.split(",")
        val channelId = line(28)//渠道id
        val channelName = line(30).replace("@","")//渠道名称
        val vmId = line(1)//自贩机id
        if("".equals(channelId)){
          println(vmId)
        }
        list .::= (channelId,vmId+","+channelName+",seven")
      })
      list.iterator
    }).reduceByKey(_+"@"+_).mapPartitions(x =>{
      var list =List[(String)]()
      x.foreach(row =>{
        val lines = row._2.split("@")
        val channelName = lines(0).split(",")(1)
        var map:Map[String,Boolean] = Map()
        for(l <- lines){
          val line = l.split(",")
          if(!map.contains(line(0))){
            map += (line(0) -> true)
          }
        }
        list .::= (row._1+","+channelName+","+map.size)
      })
      list.iterator
    }).cache()

    data.repartition(1).saveAsTextFile("/Users/seven/data/zfiNum/channel/")
  }
}
