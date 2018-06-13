package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

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
  * date     2018/6/8 下午1:53     
  */
object Operate {
  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val salveMachinePath = "/yst/sta_vem/vem_slave_machine/*"

    val logPath = "/yst/sta_vem/vem_onoff_line_log/*"
    val logMap = getLog(logPath)
    val logBv = sc.broadcast(logMap)

    val machinePath = "/yst/sta_vem/vem_machine/*"
    getMachine(machinePath,logBv)
  }


  def getSlaveMachine(path:String): util.HashMap[String,Long] ={
    val data = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      "0".equals(line(8))
    }).mapPartitions(x =>{
      var list = List[(String,Long)]()
      x.foreach(row =>{
        val line = row.split(",")
        list .::= (line(1),1L)
      })
      list.iterator
    }).reduceByKey(_+_).collect()
    val map = new util.HashMap[String,Long]()
    for(d <- data){
      map.put(d._1,d._2)
    }
    map
  }

  /**
    * 获取网点的城市信息
    * @param path
    * @return
    */
  def getCityBasic(path:String): util.HashMap[String,String] ={
    val city = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      "net".equals(line(8))
    }).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val line = row.split(",")
        hashMap.put(line(0),line(11)+"_"+line(12))//网点编号,城市
      })
      hashMap.iterator
    }).collect()
    val map = new util.HashMap[String,String]()
    for(c <- city){
      map.put(c._1,c._2)
    }
    map
  }

  /**
    * 获取机器最早日志信息
    * @param path
    * @return
    */
  def getLog(path:String): java.util.HashMap[String,String] ={
    val log = sc.textFile(path).mapPartitions(x =>{
      var list = List[(String,String)]()
      x.foreach(row =>{
        val line = row.split(",")
        val id = line(1)
        val time = line(3)
        list.::=(id,time)
      })
      list.iterator
    }).reduceByKey(_+"#"+_).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val lines = row._2.split("#")
        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var timeNum = (new Date()).getTime
        var time = ""
        for(line <- lines){
          if(timeNum > df.parse(line).getTime){
            timeNum = df.parse(line).getTime
            time = line
          }
        }
        hashMap.put(row._1,time)
      })
      hashMap.iterator
    }).collect()

    val map = new util.HashMap[String,String]()
    for(l <- log){
      map.put(l._1,l._2)
    }
    map
  }
  val mapNum = new util.HashMap[String,String]()

  def getMachine(path:String,broadcast: Broadcast[java.util.HashMap[String,String]]): Unit ={
    val machine = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      "2".equals(line(21)) && !"".equals(line(18)) && !"".equals(line(19)) && "1".equals(line(4)) && "0".equals(line(5)) && !"620".equals(line(18)) && !"101".equals(line(18)) && !"7667".equals(line(18))
    }).mapPartitions(x =>{
      var list = List[(String,String)]()
      x.foreach(row =>{
        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = new Date()
        val map = broadcast.value
        val line = row.split(",")
        val id = line(0)
        val pointId = line(19)
        val netId = line(18)
        val createTime = line(15)
        var timeOut = ""
        var timeNum = 0.0

        val time = map.get(id)
        if(time != null){
          timeOut = time
          timeNum = (date.getTime - df.parse(time).getTime) / 1000.0 / 3600.0 / 24.0
        }else{
          mapNum.put(netId,"")
        }

        val createTimeNum = (date.getTime - df.parse(createTime).getTime) / 1000.0 / 3600.0 / 24.0
        list .::= (pointId,createTime+","+createTimeNum+","+timeOut+","+timeNum)
      })
      list.iterator
    }).cache()
    machine.foreach(println)
    println(mapNum.size())
  }
}
