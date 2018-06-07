package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

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
  * date     2018/4/20 下午4:11
  *
  * 1.排除高价值订单和活动订单
  * 2.取营业天数大于30天，且近20天内销售额大于150元的小区，
  * 3.计算网点内点位的20日销量s
  * 4.以网点为单位对s做归一化处理（网点内最高销量的点位为1）得到集合Z
  * 3.以网点为单位分别计算Z的变异系数(标准差/平均值)
  * 4.输出结果，城市，网点，变异系数，20日销售额，
  */
object SalesVolume {

  private final val log = LoggerFactory.getLogger(SalesVolume.getClass)

  def main(args: Array[String]): Unit = {
    log.info("start . . .")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    log.info("jobStartTime is " + format.format(date.getTime))
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    //    val net = "/yst/seven/Downloads/nettype"
    //    val netMap = nettypeData(sc,net)
    //    val netpath = "/yst/sta_vem/vem_nettype/*"
    //    val netMap = nettypeData(sc,netpath)
    //    val netBv = sc.broadcast(netMap)


    //网点数据路径
    val pathNettype = "/yst/sta_vem/vem_nettype/*"

    //获取网点map
    val nettypeMap = salesByNettypeName(sc, pathNettype)

    //广播网点关系
    val nettypeBv = sc.broadcast(nettypeMap)

    val pointPath = "/yst/vem/info/point/*"
    val pointMap = salesPoint(sc, pointPath)
    val pointBv = sc.broadcast(pointMap)


    val netTimePath = "/yst/vem/operate/N/main/*"
    val netTimeMap = salesByMachineNum(sc, netTimePath)
    val netTimeBv = sc.broadcast(netTimeMap)

    //    val orderPath = "/yst/seven/Downloads/order"
    val orderPath = "/yst/vem/sales/order/"
    val order = salesOrderData(sc, orderPath, netTimeBv, pointBv, nettypeBv)

    order.foreach(x => println(x._2.split(",")(0)))

    //order.map(x => (x._1+","+x._2)).repartition(1).saveAsTextFile("/yst/seven/data/salesVolume/")

    //    println(coefficientOfVariation(("21260.0&10640.0&9480.0")))
    // println(order.count())
    stopWatch.stop()
    log.info("job time SalesVolume is " + stopWatch.toString)
    log.info("jobEndTime is " + format.format(date.getTime))
    log.info("job is success")
    log.info("end . . .")

  }

  /**
    * 计算两个时间搓差  单位：天
    *
    * @param startTime
    * @param endTime
    * @return
    */
  def differenceTime(startTime: Long, endTime: Long): String = {
    val num = (endTime - startTime) / 86400000
    num.toString
  }

  /**
    * 计算网点名称
    *
    * @param sc
    * @param path
    * @return
    */
  def salesByNettypeName(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesNettype is start . . .")
    val nettype = sc.textFile(path)
      .map(x => {
        val line = x.split(",")
        (line(0), line(1))
      }).collect()

    val nettypeMap = new util.HashMap[String, String]
    for (n <- nettype) {
      nettypeMap.put(n._1, n._2)
    }
    log.info("salesNettype is end . . .")
    nettypeMap
  }

  /**
    * 计算点位运营天数数
    *
    * @param path
    */
  def salesByMachineNum(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesByMachineNum is start . . .")
    val machineNum = sc.textFile(path) //0列->id,2列->运营天数
      .map(x => (x.toString.split(",")(0).replace("(", "").trim, x.toString.split(",")(2))).collect()
    val machineNumMap = new util.HashMap[String, String]
    for (m <- machineNum) {
      machineNumMap.put(m._1, m._2)
    }
    log.info("salesByMachineNum is end . . .")
    machineNumMap
  }

  /**
    * 计算机器和小区点位的关系
    *
    * @param sc
    * @param path
    */
  def nettypeData(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("nettypeData is start . . .")
    val nettype = sc.textFile(path)
      .map(x => nettypeToMap(x.toString)).collect()
    val nettypeMap = new util.HashMap[String, String]
    for (n <- nettype) {
      nettypeMap.put(n._1, n._2)
    }
    log.info("nettypeData is end . . .")
    nettypeMap
  }

  /**
    * 计算小区关系map
    *
    * @param row
    * @return
    */
  def nettypeToMap(row: String): (String, String) = {
    val line = row.toString.split(",")
    val nettypeId = line(0)
    //网点id
    val pointId = line(20) //点位id
    (pointId, nettypeId)
  }

  /**
    * 计算网点和点位之间的关系
    *
    * @param sc
    * @param path
    */
  def salesPoint(sc: SparkContext, path: String): util.HashMap[String, String] = {
    val point = sc.textFile(path)
      .map(x => {
        val line = x.toString.split(",")
        val pointId = line(0)
        val nettypeId = line(1)
        (nettypeId, pointId)
      })
      .reduceByKey(_ + "#" + _).collect()
    val pointMap = new util.HashMap[String, String]()
    for (p <- point) {
      pointMap.put(p._1, p._2)
    }
    pointMap
  }


  /**
    * 排除高价值订单和活动订单
    * 取营业天数大于30天，且近20天内销售额大于150元的小区，
    * 计算网点内点位的20日销售额s
    * 以网点为单位对s做归一化处理（网点内最高销量的点位为1）得到集合Z
    * 以网点为单位分别计算Z的变异系数(标准差/平均值)
    * 输出结果，城市，网点，变异系数，20日销售额，
    *
    * @param sc
    * @param orderPath
    * @param bv
    * @param pointBv
    * @return
    */
  def salesOrderData(sc: SparkContext, orderPath: String, bv: Broadcast[util.HashMap[String, String]]
                     , pointBv: Broadcast[util.HashMap[String, String]]
                     , netBv: Broadcast[util.HashMap[String, String]]): RDD[(String, String)] = {
    val order = sc.textFile(orderPath)
      .filter(x => ((!x.split(",")(4).equals("100.0")) && (x.split(",")(4).toDouble < 15000.0))) //排除高价值订单和活动订单
      .mapPartitions(x => {
      var list = List[(Double, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(8)
        val operateTime = bv.value.get(netId).toDouble //找到运营天数
        list.::=(operateTime, row)
      })
      list.iterator
    }).filter(x => x._1 > 30)
      .map(x => (x._2.toString.split(",")(8), x._2))
      .reduceByKey(_ + "#" + _)
      .mapPartitions(x => {
        var list = List[(String, String)]()
        x.foreach(row => {
          val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val date = new Date()
          val pointByBetMap = pointBv.value
          //点位与网点之间的关系
          var pointMap: Map[String, Int] = Map()
          val long = 1000 * 3600 * 24 * 20L
          //20天时间
          val lines = row._2.toString.split("#")
          var moneys = 0.0
          //销售金额
          var moneyMap: Map[String, Double] = Map()
          for (line <- lines) {
            val l = line.split(",")
            if (df.parse(l(6)).getTime > (date.getTime - long)) {
              moneys += l(4).toDouble
              //20日销售金额
              val m = l(4).toDouble
              if (!pointMap.contains(l(9))) {
                //20日内网点有销售记录的点位
                pointMap += (l(9) -> 0)
              }
            }
          }
          for (l <- lines) {
            val line = l.split(",")
            if (df.parse(line(6)).getTime > (date.getTime - long)) {
              for (m <- pointMap) {
                if (m._1.equals(line(9))) {
                  //计算点位销售量
                  pointMap += (m._1 -> (m._2 + 1))
                  moneyMap += (m._1 -> (moneyMap.get(m._1).getOrElse(0.0) + line(4).toDouble))
                }
              }
            }
          }
          val city = lines(0).split(",")(10)
          //城市
          var dataString = ""
          for (m <- pointMap) {
            dataString += m._2 + "&"
          }
          if (pointByBetMap.get(row._1) != null) {
            val number = pointByBetMap.get(row._1).split("#").length - pointMap.size
            if (number != 0) {
              var nums = 0
              while (number > nums) {
                dataString += "0&"
                nums += 1
              }
            }
          }
          var money = ""
          for (m <- moneyMap) {
            money += m._2 + "&"
          }
          if (!"".equals(dataString)) {
            dataString = dataString.substring(0, dataString.length - 1)
          }
          if (!"".equals(money)) {
            money = money.substring(0, money.length - 1)
          }
          var name = ""
          val netName = netBv.value.get(row._1)
          if (netName != null) {
            name = netName
          }
          //网点id，网点20日总销售金额，点位20日销售记录，城市
          list.::=(row._1 + "," + moneys, dataString + "," + city + "," + money + "," + name)
        })
        list.iterator
      }).filter(x => x._1.toString.split(",")(1).toDouble > 15000.0) //网点20日销售总金额大于150元
      .mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        //归一化后的数据
        //val normalizationData = normalization(row._2.split(",")(2))
        val city = row._2.split(",")(1)
        //城市
        val netId = row._1.split(",")(0)
        //网点
        val coefficient = coefficientOfVariation(row._2.split(",")(2))
        //变异系数
        val money = row._1.split(",")(1)
        //销售额
        val name = row._2.split(",")(3) //网点名称
        list.::=(city, netId + "," + name + "," + coefficient + "," + money)
      })
      list.iterator
    }).filter(x => {
      val line = x._2.split(",")
      val num = line(2).toDouble
      num >= 0.7
    }).cache()
    order
  }

  /**
    * 数据归一化
    * Xnorm=(X−Xmin)/(Xmax−Xmin)
    */
  def normalization(data: String): String = {
    val datas = data.split("&")
    val arr = new Array[Double](datas.length)
    for (d <- 0 until datas.length) {
      if (!"".equals(datas(d))) {
        arr(d) = datas(d).toDouble
      } else {
        arr(d) = 0.0
      }
    }
    scala.util.Sorting.quickSort(arr)
    //排序
    var processingData = ""
    //val number = arr.max - arr.min//获取最大值与最小值差
    val number = arr.max //按照最大值比例化
    for (n <- 0 until arr.length) {
      val num: Double = arr(n) / number //数据归一化
      processingData += num + "&"
    }
    if (!"".equals(processingData)) {
      processingData = processingData.substring(0, processingData.length - 1)
    }
    processingData
  }

  /**
    * 计算变异系数(标准差/平均值)
    *
    * @param data
    * @return
    */
  def coefficientOfVariation(data: String): String = {
    if (data.contains("NaN")) return "0.0"
    val datas = data.split("&")
    val arr = new Array[Double](datas.length)
    for (d <- 0 until datas.length) {
      arr(d) = datas(d).toDouble
    }
    var averageValue = 0.0 //平均值
    for (a <- arr) {
      //获得所有数据的和
      averageValue += a
    }
    averageValue = averageValue / arr.length //获得平均值
    var variance = 0.0 //平均方差
    for (a <- arr) {
      //计算方差和
      variance += salesVariance(a, averageValue)
    }
    variance = variance / arr.length //计算平均方差
    //标准差=平均方差开根号
    val coefficient = Math.sqrt(variance) / averageValue //变异系数
    coefficient.toString
  }

  /**
    * 计算方差
    * (数值-平均值)*(数值-平均值)
    */
  def salesVariance(num: Double, number: Double): Double = {
    (num - number) * (num - number)
  }
}
