package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/16 下午5:41     
  */
object SalesGoodNet {

  val conf = new SparkConf().setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val operatePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/operate/N/main/*"
    val operateMap = getNetOperateTime(operatePath)
    //广播
    val operateBv = sc.broadcast(operateMap)
    val orderPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order/*"
    val rdd = getOrderData(orderPath,operateBv)

    getNetByMoneyThan10(rdd)
    getNetByMoneyThan300(rdd)

  }

  /**
    * 获取网点运营天数,超过15天的网点
    * @param path
    * @return
    */
  def getNetOperateTime(path:String): util.HashMap[String,String] ={
    val operate = sc.textFile(path).mapPartitions(x =>{
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val line = row.replace("(","").split(",")
        val netId = line(0)//网点id
        val time = line(2)//运营时间
        hashMap.put(netId,time)
      })
      hashMap.iterator
    }).filter(x => x._2.toDouble > 15).collect()

    val operateMap = new util.HashMap[String, String]
    for(o <- operate){
      operateMap.put(o._1,o._2)
    }
    operateMap
  }
  /**
    * 获取十五天内的销售数据
    * @param path
    */
  def getOrderData(path:String,broadcast: Broadcast[util.HashMap[String,String]]): RDD[(String,Double)] ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    val long = 1000 * 3600 * 24 * 15L
    val order = sc.textFile(path).filter(x => {//选取十五天内的销售数据
      val line = x.split(",")
      val time = line(0)
      val money = line(4)//销售金额
      !"100.0".equals(money) && df.parse(time).getTime > date.getTime - long//过滤十五天以外的数据，过滤活动订单
    }).mapPartitions(x =>{
      var list = List[(String,Double)]()
      x.foreach(row =>{
        val line = row.split(",")
        val netId = line(8)//网点id
        val money = line(4).toDouble//订单金额
        list .::= (netId,money)
      })
      list.iterator
    }).reduceByKey(_+_)
      .filter(x =>{//过滤运营时间小于7天的网点
        val map = broadcast.value
        map.containsKey(x._1)
      })
      .mapPartitions(x =>{//增加运营天数
        val hashMap = new mutable.HashMap[String,Double]()
        x.foreach(row =>{
          val map = broadcast.value
          val time = map.get(row._1).toDouble
          var avgMoney = 0.0
          if(time >= 15){//获取15天平均销售额，小于15天则取实际天数
            avgMoney = row._2 / 15 / 100
          }else{
            avgMoney = row._2 / time / 100
          }
          hashMap.put(row._1,avgMoney)
        })
        hashMap.iterator
      })
      .cache()
    order
  }

  /**
    * 销量好的网点，日均销售额大于二百五(15天)
    * @param rdd
    */
  def getNetByMoneyThan300(rdd:RDD[(String,Double)]): Unit ={
    val data = rdd.filter(x => x._2 >= 250).cache()

    data.foreach(x => println(x._1))
    println("***********good**************")
  }

  /**
    * 销量差的网点，日均销售额小于十元(15天)
    * @param rdd
    */
  def getNetByMoneyThan10(rdd:RDD[(String,Double)]): Unit ={
    val data = rdd.filter(x => x._2 <= 10).cache()

    data.foreach(x => println(x._1))
    println("************bad*************")
  }
}
