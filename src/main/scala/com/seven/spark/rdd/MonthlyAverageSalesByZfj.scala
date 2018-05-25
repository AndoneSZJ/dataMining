package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/17 下午3:36
  * 按照渠道维度计算过去一年，每月台销情况
  * 2017.05-2018.04
  */
object MonthlyAverageSalesByZfj {
  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info("job is start ...")
    val stopWatch = new StopWatch()
    stopWatch.start()
    val channelPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*"
    val channelMap = getChannelByNet(channelPath)
    val channelBv = sc.broadcast(channelMap)

    val abnormalPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/result/abnormalVM/*"
    val abnormalMap = getAbnormalByZfj(abnormalPath)
    val abnormalBv = sc.broadcast(abnormalMap)

    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/DW_ZFJ_RLB_ALL/*"
    getMonthlyAverageSalesByZfj(path,channelBv,abnormalBv)
    stopWatch.stop()
    log.info("job is success timeout is " + stopWatch.toString)

  }

  /**
    * 获取网点和渠道关系信息
    * @param path
    */
  def getChannelByNet(path:String): util.HashMap[String,String] ={
    val channel = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      line.length > 62 && "1".equals(line(4))//过滤数据
    }).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val line = row.split(",")
        val netId = line(34)//网点id
        val channelId = line(28)//渠道id
        val channelName = line(30).replace("@","")//渠道名称
        hashMap.put(netId,channelId+","+channelName+",seven")
      })
      hashMap.iterator
    }).collect()

    val channelMap = new util.HashMap[String,String]()
    for(c <- channel){
      channelMap.put(c._1,c._2)
    }
    channelMap
  }


  /**
    * 获取异常刷单网点信息
    * @param path
    * @return
    */
  def getAbnormalByZfj(path:String): util.HashMap[String,Int] ={
    val abnormal = sc.textFile(path).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,Int]()
      x.foreach(row =>{
        val line = row.split(",")
        val vmId = line(0)
        hashMap.put(vmId,1)
      })
      hashMap.iterator
    }).collect()

    val abnormalMap = new util.HashMap[String,Int]()
    for(a <- abnormal){
      abnormalMap.put(a._1,a._2)
    }
    abnormalMap
  }

  /**
    * 计算2017.05-2018.04每月台销
    * @param path
    * @param broadcast
    */
  def getMonthlyAverageSalesByZfj(path:String,broadcast: Broadcast[util.HashMap[String, String]],
                                  abnormalBv: Broadcast[util.HashMap[String, Int]]): Unit ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val data = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      val time = line(4)//订单交易时间
      val netId = line(2)//网点id
      val vmId = line(1)//自贩机id
      val channelMap = broadcast.value
      val abnormalMap = abnormalBv.value
//      val channel = channelMap.get(netId)
      //过滤不属于这一年内的数据
      //过滤找不到渠道的数据
      //过滤异常自贩机
      !abnormalMap.containsKey(vmId) && channelMap.containsKey(netId) && df.parse(time).getTime >= df.parse("2017-05-01 00:00:00.0").getTime && df.parse(time).getTime < df.parse("2018-05-01 00:00:00.0").getTime
    }).mapPartitions(x =>{
      var list = List[(String,String)]()
      x.foreach(row =>{
        val line = row.split(",")
        val vmId = line(1)//自贩机id
        val netId = line(2)//网点id
        val time = line(4)//销售时间
        val money = line(5)//销售金额
        val channelMap = broadcast.value
        val channel = channelMap.get(netId)
        val channelId = channel.split(",")(0)
        val channelName = channel.split(",")(1)
        list .::= (channelId,vmId+","+time+","+money+","+channelName+",seven")
      })
      list.iterator
    }).reduceByKey(_+"@"+_).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val lines = row._2.split("@")
        var timeMap1:Map[String,Double] = Map()
        var timeMap2:Map[String,Double] = Map()
        var timeMap3:Map[String,Double] = Map()
        var timeMap4:Map[String,Double] = Map()
        var timeMap5:Map[String,Double] = Map()
        var timeMap6:Map[String,Double] = Map()
        var timeMap7:Map[String,Double] = Map()
        var timeMap8:Map[String,Double] = Map()
        var timeMap9:Map[String,Double] = Map()
        var timeMap10:Map[String,Double] = Map()
        var timeMap11:Map[String,Double] = Map()
        var timeMap12:Map[String,Double] = Map()
        val channelName = lines(0).split(",")(3)
        for(l <- lines){
          val line = l.split(",")
          val time = line(1)
          val vmId = line(0)
          val money = line(2).toDouble
          val longTime = df.parse(time).getTime
          if(longTime < df.parse("2017-06-01 00:00:00.0").getTime){
            if(timeMap1.contains(vmId)){
              val moneys:Double = timeMap1.getOrElse(vmId, 0.0) + money
              timeMap1 += (vmId -> moneys)
            }else{
              timeMap1 += (vmId -> money)
            }
          }else if(longTime < df.parse("2017-07-01 00:00:00.0").getTime){
            if(timeMap2.contains(vmId)){
              val moneys:Double = timeMap2.getOrElse(vmId, 0.0) + money
              timeMap2 += (vmId -> moneys)
            }else{
              timeMap2 += (vmId -> money)
            }
          }else if(longTime < df.parse("2017-08-01 00:00:00.0").getTime){
            if(timeMap3.contains(vmId)){
              val moneys:Double = timeMap3.getOrElse(vmId, 0.0) + money
              timeMap3 += (vmId -> moneys)
            }else{
              timeMap3 += (vmId -> money)
            }
          }else if(longTime < df.parse("2017-09-01 00:00:00.0").getTime){
            if(timeMap4.contains(vmId)){
              val moneys:Double = timeMap4.getOrElse(vmId, 0.0) + money
              timeMap4 += (vmId -> moneys)
            }else{
              timeMap4 += (vmId -> money)
            }
          }else if(longTime < df.parse("2017-10-01 00:00:00.0").getTime){
            if(timeMap5.contains(vmId)){
              val moneys:Double = timeMap5.getOrElse(vmId, 0.0) + money
              timeMap5 += (vmId -> moneys)
            }else{
              timeMap5 += (vmId -> money)
            }
          }else if(longTime < df.parse("2017-11-01 00:00:00.0").getTime){
            if(timeMap6.contains(vmId)){
              val moneys:Double = timeMap6.getOrElse(vmId, 0.0) + money
              timeMap6 += (vmId -> moneys)
            }else{
              timeMap6 += (vmId -> money)
            }
          }else if(longTime < df.parse("2017-12-01 00:00:00.0").getTime){
            if(timeMap7.contains(vmId)){
              val moneys:Double = timeMap7.getOrElse(vmId, 0.0) + money
              timeMap7 += (vmId -> moneys)
            }else{
              timeMap7 += (vmId -> money)
            }
          }else if(longTime < df.parse("2018-01-01 00:00:00.0").getTime){
            if(timeMap8.contains(vmId)){
              val moneys:Double = timeMap8.getOrElse(vmId, 0.0) + money
              timeMap8 += (vmId -> moneys)
            }else{
              timeMap8 += (vmId -> money)
            }
          }else if(longTime < df.parse("2018-02-01 00:00:00.0").getTime){
            if(timeMap9.contains(vmId)){
              val moneys:Double = timeMap9.getOrElse(vmId, 0.0) + money
              timeMap9 += (vmId -> moneys)
            }else{
              timeMap9 += (vmId -> money)
            }
          }else if(longTime < df.parse("2018-03-01 00:00:00.0").getTime){
            if(timeMap10.contains(vmId)){
              val moneys:Double = timeMap10.getOrElse(vmId, 0.0) + money
              timeMap10 += (vmId -> moneys)
            }else{
              timeMap10 += (vmId -> money)
            }
          }else if(longTime < df.parse("2018-04-01 00:00:00.0").getTime){
            if(timeMap11.contains(vmId)){
              val moneys:Double = timeMap11.getOrElse(vmId, 0.0) + money
              timeMap11 += (vmId -> moneys)
            }else{
              timeMap11 += (vmId -> money)
            }
          }else{
            if(timeMap12.contains(vmId)){
              val moneys:Double = timeMap12.getOrElse(vmId, 0.0) + money
              timeMap12 += (vmId -> moneys)
            }else{
              timeMap12 += (vmId -> money)
            }
          }
        }

        var avgMoney1 = 0.0
        var avgMoney2 = 0.0
        var avgMoney3 = 0.0
        var avgMoney4 = 0.0
        var avgMoney5 = 0.0
        var avgMoney6 = 0.0
        var avgMoney7 = 0.0
        var avgMoney8 = 0.0
        var avgMoney9 = 0.0
        var avgMoney10 = 0.0
        var avgMoney11 = 0.0
        var avgMoney12 = 0.0
        //2017.05
        for(t <- timeMap1){
          avgMoney1 += t._2
        }
        avgMoney1 = avgMoney1 / timeMap1.size
        //2017.06
        for(t <- timeMap2){
          avgMoney2 += t._2
        }
        avgMoney2 = avgMoney2 / timeMap2.size
        //2017.07
        for(t <- timeMap3){
          avgMoney3 += t._2
        }
        avgMoney3 = avgMoney3 / timeMap3.size
        //2017.08
        for(t <- timeMap4){
          avgMoney4 += t._2
        }
        avgMoney4 = avgMoney4 / timeMap4.size
        //2017.09
        for(t <- timeMap5){
          avgMoney5 += t._2
        }
        avgMoney5 = avgMoney5 / timeMap5.size
        //2017.10
        for(t <- timeMap6){
          avgMoney6 += t._2
        }
        avgMoney6 = avgMoney6 / timeMap6.size
        //2017.11
        for(t <- timeMap7){
          avgMoney7 += t._2
        }
        avgMoney7 = avgMoney7 / timeMap7.size
        //2017.12
        for(t <- timeMap8){
          avgMoney8 += t._2
        }
        avgMoney8 = avgMoney8 / timeMap8.size
        //2018.01
        for(t <- timeMap9){
          avgMoney9 += t._2
        }
        avgMoney9 = avgMoney9 / timeMap9.size
        //2018.02
        for(t <- timeMap10){
          avgMoney10 += t._2
        }
        avgMoney10 = avgMoney10 / timeMap10.size
        //2018.03
        for(t <- timeMap11){
          avgMoney11 += t._2
        }
        avgMoney11 = avgMoney11 / timeMap11.size
        //2018.04
        for(t <- timeMap12){
          avgMoney12 += t._2
        }
        avgMoney12 = avgMoney12 / timeMap12.size

        val result =channelName+ ",2017.05:"+avgMoney1+",2017.06:"+avgMoney2+",2017.07:"+avgMoney3+",2017.08:"+avgMoney4+
          ",2017.09:"+avgMoney5+",2017.10:"+avgMoney6+",2017.11:"+avgMoney7+",2017.12:"+avgMoney8+",2018.01:"+avgMoney9+
          ",2018.02:"+avgMoney10+",2018.03:"+avgMoney11+",2018.04:"+avgMoney12
        hashMap.put(row._1,result)
      })
      hashMap.iterator
    })//.cache()
    data.take(3)
    //data.repartition(1).saveAsTextFile("hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/seven/monthlyAverageSalesByZfj/")
  }
}
