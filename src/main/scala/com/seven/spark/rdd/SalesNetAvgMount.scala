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
  * author   seven  
  * email    sevenstone@yeah.net
  * date     2018/5/23 下午4:19
  * 运营天数 > 15天
  * 7日销售数据
  * 网点，日均网销（去除大额，活动），户数，入住户数，小区年限，物业费，点位数，机器数
  */
object SalesNetAvgMount {

  private final val log = LoggerFactory.getLogger(this.getClass)


  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    log.info("job is start . . . ")
    val stopWatch = new StopWatch()
    stopWatch.start()

    val operatePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/operate/N/main/*"
    val operateMap = getOperateTimeByNet(operatePath)
    val operateBv = sc.broadcast(operateMap)

    val basicNetMap = NetTypeUtils.salesNetData(sc)
    val basicNetBv = sc.broadcast(basicNetMap)

    val orderPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order/*"
    getOrderDataNet(orderPath, operateBv, basicNetBv)
    stopWatch.stop()

    log.info("job is success spend time is " + stopWatch.toString)
  }

  /**
    * 获取网点运营天数,点位数,机器数,超过15天的网点
    *
    * @param path
    * @return
    */
  def getOperateTimeByNet(path: String): util.HashMap[String, String] = {
    val operate = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replace("(", "").split(",")
        val netId = line(0)
        //网点id
        val time = line(2)
        //运营时间
        val pointNum = line(10)
        //点位数
        val machineNum = line(4) //机器数
        hashMap.put(netId, time + "@" + pointNum + "," + machineNum)
      })
      hashMap.iterator
    }).filter(x => x._2.split("@")(0).toDouble > 15).collect()

    val operateMap = new util.HashMap[String, String]
    for (o <- operate) {
      operateMap.put(o._1, o._2.split("@")(1))
    }
    operateMap
  }


  /**
    * 获取7天内的销售数据
    *
    * @param path
    */
  def getOrderDataNet(path: String, operateBv: Broadcast[util.HashMap[String, String]], basicNetBv: Broadcast[util.HashMap[String, String]]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    val long = 1000 * 3600 * 24 * 7L
    val order = sc.textFile(path).filter(x => {
      //选取7天内的销售数据
      val line = x.split(",")
      val time = line(0)
      val money = line(4)
      //销售金额
      val map = operateBv.value
      map.containsKey(line(8)) && money.toDouble < 15000 && !"100.0".equals(money) && df.parse(time).getTime > date.getTime - long //过滤7天以外的数据，过滤运营天数不足的网点，过滤活动订单，大额订单
    }).mapPartitions(x => {
      var list = List[(String, Double)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(8)
        //网点id
        val city = line(10)
        //城市
        val money = line(4).toDouble //订单金额
        list.::=(netId + "," + city, money)
      })
      list.iterator
    }).reduceByKey(_ + _)
      .mapPartitions(x => {
        //增加运营天数
        var list = List[String]()
        x.foreach(row => {
          val netId = row._1.split(",")(0)
          val operateMap = operateBv.value
          val basicNetMap = basicNetBv.value
          val operate = operateMap.get(netId)

          val net = basicNetMap.get(netId)
          var netBasic = ""

          if (net == null) {
            netBasic = ",,,,," + operate
          } else {
            val basic = net.split(",")
            val netName = basic(1)
            //网点名称
            val householdTotalNum = basic(25)
            //小区户数
            val householdCheckInNum = basic(16)
            //入住户数
            val propertyCosts = basic(22)
            //物业费
            val time = (date.getTime - df.parse(basic(21)).getTime) / (1000 * 60 * 60 * 24L) / 365 //小区运行多少年
            netBasic = netName + "," + householdTotalNum + "," + householdCheckInNum + "," + time + "," + propertyCosts + "," + operate
          }
          var avgMoney: Double = 0.0
          avgMoney = row._2 / 7 / 100
          val money = avgMoney.formatted("%.2f")
          list.::=(row._1 + "," + money + "," + netBasic)
        })
        list.iterator
      }).cache()
    order.repartition(1).saveAsTextFile("/Users/seven/data/orderData/SalesNetAvgMount/")
  }


}
