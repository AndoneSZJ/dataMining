package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.seven.spark.sparksql.NetTypeUtils
import com.seven.spark.utils.Utils
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

    val operatePath = "/yst/vem/operate/N/main/*"
    val operateMap = getOperateTimeByNet(operatePath)
    val operateBv = sc.broadcast(operateMap)

    val basicNetMap = NetTypeUtils.salesNetData(sc)
    val basicNetBv = sc.broadcast(basicNetMap)

    val basicPointMap = NetTypeUtils.salesPointData(sc)
    val basicPointBv = sc.broadcast(basicPointMap)


    val orderPath = "/yst/vem/sales/order/*"
    getOrderDataNet(orderPath, operateBv, basicNetBv,basicPointBv)
    //getOrderDataNetByPointThan30(orderPath, operateBv, basicNetBv)
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
        //网点id
        val netId = line(0)
        //运营时间
        val time = line(2)
        //点位数
        val pointNum = line(10)
        //机器数
        val machineNum = line(4)
        hashMap.put(netId, time + "@" + pointNum + "," + machineNum)
      })
      hashMap.iterator
    })
      //.filter(x => x._2.split("@")(0).toDouble > 15)
      .collect()

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
  def getOrderDataNet(path: String, operateBv: Broadcast[util.HashMap[String, String]],
                      basicNetBv: Broadcast[util.HashMap[String, String]],
                      basicPointBv: Broadcast[util.HashMap[String, String]]): Unit = {
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
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        //网点id
        val netId = line(8)
        //城市
        val city = line(10)
        //点位
        val pointId = line(9)
        val money = line(4).toDouble //订单金额
        list.::=(netId + "," + city, pointId+","+money)
      })
      list.iterator
    }).reduceByKey(_+"@"+ _)
      .mapPartitions(x => {
        //增加运营天数
        var list = List[String]()
        x.foreach(row => {
          val lines = row._2.split("@")
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

          //获取点位信息
          var map:Map[String,Double] = Map()//所有
          var groundMap:Map[String,Double] = Map()//地面
          var undergroundMap:Map[String,Double] = Map()//地下
          for(l <- lines){
            val line = l.split(",")
            if(map.contains(line(0))){
              map += (line(0) -> (map.get(line(0)).getOrElse(0.0)+line(1).toDouble))
            }else{
              map += (line(0) -> line(1).toDouble)
            }
            val point = basicPointBv.value.get(line(0))
            if(point != null){
              val isPosition = point.split(",")(14)//是否地上
              if("1".equals(isPosition)){//1地下，0地上
                if(undergroundMap.contains(line(0))){
                  undergroundMap += (line(0) -> (undergroundMap.get(line(0)).getOrElse(0.0)+line(1).toDouble))
                }else{
                  undergroundMap += (line(0) -> line(1).toDouble)
                }
              }else{
                if(groundMap.contains(line(0))){
                  groundMap += (line(0) -> (groundMap.get(line(0)).getOrElse(0.0)+line(1).toDouble))
                }else{
                  groundMap += (line(0) -> line(1).toDouble)
                }
              }
            }
            avgMoney += line(1).toDouble
          }

          var groundThan30 = 0
          var groundThan60 = 0
          var groundThan100 = 0
          for(m <- groundMap){
            val moneyAvg = m._2 / 7 / 100
            if(moneyAvg >= 30){
              groundThan30 += 1
            }
            if(moneyAvg >= 60){
              groundThan60 += 1
            }
            if(moneyAvg >= 100){
              groundThan100 += 1
            }
          }


          val ground = groundThan100+"/"+groundThan60+"/"+groundThan30+"/"+groundMap.size

          var undergroundThan30 = 0
          var undergroundThan60 = 0
          var undergroundThan100 = 0
          for(m <- undergroundMap){
            val moneyAvg = m._2 / 7 / 100
            if(moneyAvg >= 30){
              undergroundThan30 += 1
            }
            if(moneyAvg >= 60){
              undergroundThan60 += 1
            }
            if(moneyAvg >= 100){
              undergroundThan100 += 1
            }
          }


          val underground = undergroundThan100+"/"+undergroundThan60+"/"+undergroundThan30+"/"+undergroundMap.size

          var numThan30 = 0
          var numThan60 = 0
          var numThan100 = 0
          for(m <- map){
            val moneyAvg = m._2 / 7 / 100
            if(moneyAvg >= 30){
              numThan30 += 1
            }
            if(moneyAvg >= 60){
              numThan60 += 1
            }
            if(moneyAvg >= 100){
              numThan100 += 1
            }
          }


          val all = numThan100+"/"+numThan60+"/"+numThan30+"/"+map.size



          avgMoney = avgMoney / 7 / 100

          val money = avgMoney.formatted("%.2f")

          list.::=(row._1 + "," + money + "," + netBasic+","+all+","+ground+","+underground)
        })
        list.iterator
      }).cache()
//    order.repartition(1).saveAsTextFile("/yst/seven/data/orderData/SalesNetAvgMount/")
    Utils.saveHdfs(order,sc,"/yst/seven/data/orderData/SalesNetAvgMount/")
  }


  /**
    * 获取7天内的销售数据
    * 网点，日均超过30，点位数
    * @param path
    */
  def getOrderDataNetByPointThan30(path: String, operateBv: Broadcast[util.HashMap[String, String]], basicNetBv: Broadcast[util.HashMap[String, String]]): Unit = {
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
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(8)
        val pointId = line(9)
        //网点id
        val city = line(10)
        //城市
        val money = line(4).toDouble //订单金额
        list.::=(netId + "," + city, pointId+","+money)
      })
      list.iterator
    }).reduceByKey(_+"@"+_)
      .mapPartitions(x =>{
        var list = List[String]()
        x.foreach(row =>{
          val lines = row._2.split("@")
        //增加运营天数
          val netId = row._1.split(",")(0)
          val operateMap = operateBv.value
          val basicNetMap = basicNetBv.value
          val operate = operateMap.get(netId)

          var map:Map[String,Double] = Map()
          for(l <- lines){
            val line = l.split(",")
            if(map.contains(line(0))){
              map += (line(0) -> (map.get(line(0)).getOrElse(0.0)+line(1).toDouble))
            }else{
              map += (line(0) -> line(1).toDouble)
            }
          }

          var numThan30 = 0
          var numThan60 = 0
          var numThan100 = 0
          for(m <- map){
            val avgMoney = m._2 / 7 / 100
            if(avgMoney >= 30){
              numThan30 += 1
            }
            if(avgMoney >= 60){
              numThan60 += 1
            }
            if(avgMoney >= 100){
              numThan100 += 1
            }
          }

          var pointNum = ""
          if(operate != null){
            pointNum = operate.split(",")(0)
          }

          val net = basicNetMap.get(netId)
          var netBasic = ""

          if (net == null) {
            netBasic = ","+numThan100+"/"+numThan60+"/" + numThan30+"/" + pointNum
          } else {
            val basic = net.split(",")
            //网点名称
            val netName = basic(1)
            //小区户数
            //val householdTotalNum = basic(25)
            //入住户数
            //val householdCheckInNum = basic(16)
            //物业费
            //val propertyCosts = basic(22)
            //val time = (date.getTime - df.parse(basic(21)).getTime) / (1000 * 60 * 60 * 24L) / 365 //小区运行多少年
            //netBasic = netName + "," + householdTotalNum + "," + householdCheckInNum + "," + time + "," + propertyCosts + "," + operate
            netBasic = (netName +","+numThan100+"/"+numThan60+"/" + numThan30+"/"+pointNum)
          }

//          var avgMoney: Double = 0.0
//          avgMoney = row._2 / 7 / 100
//          val money = avgMoney.formatted("%.2f")
//          list.::=(row._1 + "," + money + "," + netBasic)
          list .::= (row._1 +","+netBasic)
        })
        list.iterator
      }).cache()
    Utils.saveHdfs(order,sc,"/yst/seven/data/orderData/getOrderDataNetByPointThan30/")
//    order.repartition(1).saveAsTextFile("/yst/seven/data/orderData/getOrderDataNetByPointThan30/")
  }


}
