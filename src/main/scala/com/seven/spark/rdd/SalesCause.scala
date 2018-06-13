package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.seven.spark.sparksql.NetTypeUtils
import org.apache.commons.lang.time.StopWatch
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
  * date     2018/6/11 上午9:51
  *
  * 计算销量原因与开水机的关系
  */
object SalesCause {
  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info(this.getClass.getSimpleName + " is start . . .")
    val stopWatch = new StopWatch()
    stopWatch.start()

    val operatePathByNet = "/yst/vem/operate/N/main/*"
    val netMap = getOperateByNet(operatePathByNet)
    val netBv = sc.broadcast(netMap)

    val netBasicMap = NetTypeUtils.salesNetData(sc)
    val netBasicBv = sc.broadcast(netBasicMap)

    val orderPath = "/yst/vem/sales/order/*"
    getSalesCauseByNet(orderPath,netBv,netBasicBv)

    stopWatch.stop()
    log.info(this.getClass.getSimpleName + "spend time is " + stopWatch.toString)
    log.info(this.getClass.getSimpleName + " is end . . .")
  }

  /**
    * 获取点位运营天数,超过20天的点位
    *
    * @param path
    * @return
    */
  def getOperateByNet(path: String): util.HashMap[String, String] = {
    val operate = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replaceAll("[()]","").split(",")
        //网点id
        val netId = line(0)
        //运营时间
        val time = line(2)
        hashMap.put(netId, time)
      })
      hashMap.iterator
    }).filter(x => x._2.toDouble > 20)
      .collect() //选取运营时间大于20天的点位

    val operateMap = new util.HashMap[String, String]
    for (o <- operate) {
      operateMap.put(o._1, o._2)
    }
    operateMap
  }

  /**
    * 获取20天内网点的销售量
    * @param path
    * @return
    */
  def getSalesCauseByNet(path: String,broadcast: Broadcast[util.HashMap[String,String]],
                         netBasicBv:Broadcast[util.HashMap[String,String]]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    //7天时间搓
    val long = 1000 * 3600 * 24 * 20L

    val order = sc.textFile(path).cache()

    val data = order.mapPartitions(x =>{
      var list = List[(String,Integer)]()
      x.foreach(row =>{
        val line = row.split(",")
        val netId = line(8)//网点id
        val shopId = line(11)//商品id
        var isFlag = 0
        if("228".equals(shopId)){//判断是否是开水机
          isFlag += 1
        }
       list .::= (netId,isFlag)
      })
      list.iterator
    }).reduceByKey(_+_).filter(x => x._2 > 0).collect()//过滤没有卖出过开水机网点

    //网点,卖出开水机的数量
    val dataMap = new util.HashMap[String,Integer]()
    for(d <- data){
      dataMap.put(d._1,d._2)
    }

    val rdd = order.filter(x => {
      //选取20天内的销售数据
      val line = x.split(",")
      val time = line(0)
      val money = line(4)
      !"100.0".equals(money) && df.parse(time).getTime > date.getTime - long //过滤20天以外的数据，过滤活动订单
    }).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        //网点id
        val netId = line(8)
        list.::=(netId, row.replaceAll("[()#]", ""))
      })
      list.iterator
    }).reduceByKey(_ + "#" + _).filter(x => {
      val operateMap = broadcast.value
      operateMap.containsKey(x._1)//过滤不在规定运营时间内的网点
    }).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val lines = row._2.split("#")
        var moneys = 0.0//总销售量
        var isFlag = 0//判断有没有卖过开水机
        for (l <- lines) {
          val line = l.split(",")
          val shopId = line(11) //商品id
          val money = line(4).toDouble //销售金额
          if ("228".equals(shopId)) {
            isFlag += 1
          }else{
            moneys += money
          }
        }
        moneys = moneys / 100
        hashMap.put(row._1,isFlag+","+moneys)
      })
      hashMap.iterator
    }).filter(x =>{
      val line = x._2.split(",")
      !"0".equals(line(0))//过滤没有销售过开水机的网点
    }).mapPartitions(x =>{
      var list = List[String]()
      x.foreach(row =>{
        //网点id,网点名称,城市,开水机销量,销售额
        val netBasicMap = netBasicBv.value
        val net = netBasicMap.get(row._1)
        var netName = ""
        var city = ""
        if(net != null){
          val line = net.split(",")
          netName = line(1)
          city = line(6)+"_"+line(7)
        }
        list .::= (row._1+","+netName+","+city+","+row._2)
      })
      list.iterator
    }).cache()

    rdd.foreach(println)
  }


}
