package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.seven.spark.sparksql.NetTypeUtils
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * author seven
  * time   2018-04-19
  * 台销（7日销量/机器数）,
  * 城市,网点,点位,
  * 机器数,坏机器数,产品数,
  * 最后一次销售距离今天的时间,运营时间,购买人数,复购人数,
  * 复购平均间隔,所在网点的小区时间,户数,物业费//net_community
  */
object Clustering {
  private final val log = LoggerFactory.getLogger(Clustering.getClass)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    log.info("start . . .")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    log.info("jobStartTime is " + format.format(date.getTime))
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()

    //机器数路径
    val pathMachine = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/operate/P/main/"
    //获取map
    val machineMap = clusteringByMachineNum(sc, pathMachine)
    //广播
    val machineBv = sc.broadcast(machineMap)

    //产品数
    val pathDisp = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/disp/point/static/main/"
    val dispMap = clusteringByDisp(sc, pathDisp)
    val dispBv = sc.broadcast(dispMap)


    //    //小区数据路径
    //    val pathCommunity = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/net_community/*"
    //    val communityMap = clusteringByCommunity(sc,pathCommunity)
    //    val communityBv = sc.broadcast(communityMap)

    //网点数据路径
    val pathNettype = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_nettype/*"

    //获取网点map
    val nettypeMap = clusteringNettype(sc, pathNettype)

    //广播网点关系
    val nettypeBv = sc.broadcast(nettypeMap)

    val srcPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/operate/N/main/"

    val pointNumMap = clusteringPointNum(sc, pathNettype, srcPath)
    val pointNumBv = sc.broadcast(pointNumMap)

    val userPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/user/main/"
    val userMap = clusteringByUser(sc, userPath)
    val userBv = sc.broadcast(userMap)


    val pointMap = NetTypeUtils.salesPointData(sc)
    //获取点位信息
    val netMap = NetTypeUtils.salesNetData(sc)
    //获取网点信息
    val pointDataBv = sc.broadcast(pointMap)
    val netDataBv = sc.broadcast(netMap)


    //订单数据路径
    val orderPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order/"

    val orderMap = clusteringOrder(sc, orderPath, machineBv, dispBv, nettypeBv, userBv, pointDataBv, netDataBv)

    val orderBv = sc.broadcast(orderMap)

    val activityUserMap = activityUser(sc, orderPath)
    val activityUserBv = sc.broadcast(activityUserMap)


    val pointPath = "/Users/seven/data/pointData/*"

    val data = pointCommunity(sc, pointPath, orderBv, machineBv, dispBv, activityUserBv, pointDataBv, netDataBv, pointNumBv)


    val goodNetThan250Path = "/Users/seven/data/orderData/goodNetThan250.csv"
    val goodNetThan250SavePath = "/Users/seven/data/orderData/goodNetThan250/"
    goodNetThan250(sc, goodNetThan250Path, data, goodNetThan250SavePath)
    val goodPointThan60Path = "/Users/seven/data/orderData/goodPointThan60.csv"
    val goodPointThan60SavePath = "/Users/seven/data/orderData/goodPointThan60/"
    goodNetThan250(sc, goodPointThan60Path, data, goodPointThan60SavePath)
    val badPointThan5Path = "/Users/seven/data/orderData/badPointThan5.csv"
    val badPointThan5SavePath = "/Users/seven/data/orderData/badPointThan5/"
    goodNetThan250(sc, badPointThan5Path, data, badPointThan5SavePath)
    val badNetThan10Path = "/Users/seven/data/orderData/badNetThan10.csv"
    val badNetThan10SavePath = "/Users/seven/data/orderData/badNetThan10/"
    goodNetThan250(sc, badNetThan10Path, data, badNetThan10SavePath)
    val badPointNullPath = "/Users/seven/data/orderData/badPointNull.csv"
    val badPointNullSavePath = "/Users/seven/data/orderData/badPointNull/"
    goodNetThan250(sc, badPointNullPath, data, badPointNullSavePath)
    val variationPath = "/Users/seven/data/orderData/variation.csv"
    val variationSavePath = "/Users/seven/data/orderData/variation/"
    goodNetThan250(sc, variationPath, data, variationSavePath)

    //    goodNetThan250(sc,goodNetThan250Path,data)


    //    variationNet(sc,variationPath,data)


    //data.foreach(x => (println(x._1+":"+x._2)))

    //data.map(x => x._2).repartition(1).saveAsTextFile("/Users/seven/data/clustering/")


    stopWatch.stop()
    log.info("job time consuming is " + stopWatch.toString)

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
  def timeByDifference(startTime: Long, endTime: Long): String = {
    val num = (endTime - startTime) / 86400000
    num.toString
  }

  /**
    * 计算点位活动用户
    *
    * @param sc
    * @param path
    */
  def activityUser(sc: SparkContext, path: String): util.HashMap[String, String] = {
    val activity = sc.textFile(path).filter(x => {
      val line = x.toString.split(",")
      "100.0".equals(line(4))
    }).map(x => {
      val line = x.toString.split(",")
      (line(9), line(5)) //点位id,用户id
    }).reduceByKey(_ + "&" + _).map(x => {
      val lines = x._2.toString.split("&")
      var map: Map[String, Int] = Map()
      for (line <- lines) {
        if (!map.contains(line)) {
          map += (line -> 1)
        }
      }
      (x._1, map.size)
    }).collect()

    val activityUserMap = new util.HashMap[String, String]()

    for (a <- activity) {
      activityUserMap.put(a._1, a._2.toString)
    }
    activityUserMap
  }

  /**
    * 计算机器数
    *
    * @param path
    */
  def clusteringByMachineNum(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("clusteringByMachineNum is start . . .")
    val machineNum = sc.textFile(path)
      .map(x => (x.toString.split(",")(0).replace("(", "").trim, x.toString.replace(")", ""))).collect()
    val machineNumMap = new util.HashMap[String, String]
    for (m <- machineNum) {
      machineNumMap.put(m._1, m._2)
    }
    log.info("clusteringByMachineNum is end . . .")
    machineNumMap
  }

  /**
    * 计算会员数
    *
    * @param sc
    * @param path
    * @return
    */
  def clusteringByUser(sc: SparkContext, path: String): util.HashMap[String, String] = {
    val user = sc.textFile(path).map(x => {
      val line = x.toString().split(",")
      (line(0).replace("(", ""), line(20))
    }).collect()
    val userMap = new util.HashMap[String, String]()
    for (u <- user) {
      userMap.put(u._1, u._2)
    }
    userMap
  }

  /**
    * 获取产品数
    *
    * @param sc
    * @param path
    * @return
    */
  def clusteringByDisp(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("clusteringByDisp is start . . .")
    val disp = sc.textFile(path).map(x => (x.toString.split(",")(2), x.toString.split(",")(3) + "&" + x.toString.split(",")(8))).collect()
    val dispMap = new util.HashMap[String, String]()
    for (d <- disp) {
      dispMap.put(d._1, d._2)
    }
    log.info("clusteringByDisp is end . . .")
    dispMap
  }

  /**
    * 点位所在网点的总点位数,总机器数
    *
    * @param sc
    * @param path
    * @return
    */
  def clusteringPointNum(sc: SparkContext, path: String, srcPath: String): util.HashMap[String, String] = {
    log.info("clusteringPointNum is start . . .")
    val pointNum = sc.textFile(path).filter(x => {
      val line = x.toString.split(",")
      "point".equals(line(8)) && "0".equals(line(7))
    }).mapPartitions(x => {
      var list = List[(String, Int)]()
      x.foreach(row => {
        val line = row.split(",")
        list.::=(line(2), 1) //获取点位所属网点,计数
      })
      list.iterator
    }).reduceByKey(_ + _).collect()

    val pointMap = new util.HashMap[String, Int]()
    for (p <- pointNum) {
      pointMap.put(p._1, p._2)
    }

    val net = sc.textFile(srcPath).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.split(",")
        val netId = line(0).replace("(", "")
        val pointNumber = pointMap.get(netId)
        list.::=(netId, pointNumber + "," + line(4)) //查询网点点位数,机器数
      })
      list.iterator
    }).collect()

    val map = new util.HashMap[String, String]()
    for (n <- net) {
      map.put(n._1, n._2)
    }
    log.info("clusteringPointNum is end . . .")
    map
  }

  /**
    * 计算省、市、区、小区名称
    *
    * @param path
    */
  def clusteringByCommunity(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("clusteringByCommunity is start . . .")
    val community = sc.textFile(path)
      .map(x => communityToMapPair(x.toString)).collect()

    val communityMap = new util.HashMap[String, String]
    for (c <- community) {
      communityMap.put(c._1, c._2)
    }
    log.info("clusteringByCommunity is end . . .")
    communityMap
  }

  /**
    * 计算小区信息map
    *
    * @param row
    * @return
    */
  def communityToMapPair(row: String): (String, String) = {
    val line = row.toString.split(",")
    val communityId = line(0)
    //小区id
    val communityName = line(1)
    //小区名称
    val city = line(4) + "_" + line(5)
    //省区_市区
    val peoples = line(9)
    //入住户数
    val money = line(15)
    //物业费
    val createTime = line(20) //创建时间
    (communityId, communityName + "&" + city + "&" + peoples + "&" + money + "&" + createTime)
  }

  /**
    * 计算机器和小区点位的关系
    *
    * @param sc
    * @param path
    */
  def clusteringNettype(sc: SparkContext, path: String): util.HashMap[String, String] = {
    log.info("salesNettype is start . . .")
    val nettype = sc.textFile(path)
      .map(x => clusteringNettypeToMap(x.toString)).collect()

    val nettypeMap = new util.HashMap[String, String]
    for (n <- nettype) {
      nettypeMap.put(n._1, n._2)
    }
    log.info("salesNettype is end . . .")
    nettypeMap
  }

  /**
    * 计算小区关系map
    *
    * @param row
    * @return
    */
  def clusteringNettypeToMap(row: String): (String, String) = {
    val line = row.toString.split(",")
    val nettypeId = line(0)
    //网点id
    val createtime = line(4)
    //创建时间
    val name = line(1)
    //网点名称
    val city = line(11) + "_" + line(12)
    //省区_市区
    val pointId = line(20) //小区id
    (nettypeId, pointId + "&" + createtime + "&" + city + "&" + name)
  }


  /**
    * 聚合计算
    *
    * @param sc
    * @param path
    * @param machineBv
    * @param dispBv
    * @return
    */
  def clusteringOrder(sc: SparkContext, path: String, machineBv: Broadcast[util.HashMap[String, String]],
                      dispBv: Broadcast[util.HashMap[String, String]],
                      nettypeBv: Broadcast[util.HashMap[String, String]],
                      userBv: Broadcast[util.HashMap[String, String]],
                      pointDataBv: Broadcast[util.HashMap[String, String]],
                      netDataBv: Broadcast[util.HashMap[String, String]]): util.HashMap[String, String] = {
    val sss = sc.textFile(path)
    println(sss.first())
    val order = sc.textFile(path).filter(x => {
      val line = x.split(",")
      val long = 1000 * 3600 * 24 * 7L
      //七天时间
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = new Date()
      df.parse(line(6)).getTime > (date.getTime - long) //判断是否是七天内的订单
    }).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.toString.split(",")
        val foundTime = line(0)
        //创建时间
        val amountOfPayment = line(4)
        //支付金额
        val paymentAccount = line(5)
        //支付账号
        val paymentTime = line(6)
        //支付时间
        val nettypeId = line(8)
        //网点id
        val pointId = line(9)
        //点位id
        val playMoney = line(3) //支付方式

        //id,名称,网点id,创建时间,状态(0,1),类型(点位、网点),省,市,区  0-8
        //地址,运营商,纬度,经度,点位序号,点位类型,覆盖户数,入住户数     9-16
        //电信信号强度,移动信号强度,联通信号强度,信号运营商,信号解决方案   17-21

        val pointMap = pointDataBv.value
        //获取点位信息
        var pointData = pointMap.get(pointId)


        if (pointData == null) {
          println(pointId)
          pointData = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,seven"
        }
        val point = pointData.split(",")
        //        val createTime = point(3)//小区点位创建时间
        val pointName = point(1) //点位名


        //id,名称,网点id,创建时间,状态(0,1),类型(点位、网点),省,市,区    0-8
        //地址,运营商,纬度,经度,渠道类型id,物业id,商圈备注               9-15
        //入住户数,楼栋总数,地下停车场数量,地下停车场总车位数               16-19
        //车库电梯停总数,交房日期,物业费,楼盘价格,有无自主终端                20-24
        val netMap = netDataBv.value
        var net = netMap.get(nettypeId)
        if (net == null) {
          println(nettypeId)
          net = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,seven"
        }
        val netData = net.split(",")
        val city = netData(6) + "_" + netData(7)
        //城市
        val netName = netData(1) //网点名称
        val peoples = netData(16)
        //小区户数
        val money = netData(22)
        //小区物业费
        val createTime = netData(21)
        //小区交房时间
        // 最早运营时间,运营天数,主机数量,机器数量,日均掉线5次以上的主机数,近7日日均掉线5次以上的主机数,近7日日均掉线20次以上的主机数,日均掉线时长超过6小时的主机数,日均掉线超过10次的主机数
        val machineMap = machineBv.value
        val machine = machineMap.get(pointId)

        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = new Date()
        var timeYear: Double = 0.0
        if (!"".equals(createTime)) {
          timeYear = (date.getTime - df.parse(createTime).getTime) / (1000 * 60 * 60 * 24L) / 365 //小区运行多少年
        }


        var timeNum = ""
        //运营天数
        var machineNum = ""
        //机器数量
        var machineNumber = "" //七天机器离线数（坏机器数量）

        if (machine != null) {
          //判断根据点位是否找到对应机器信息,没有就给定默认值
          val sss = machine.split(",")
          timeNum = sss(2) //运营天数
          machineNum = sss(4) //机器数量
          machineNumber = sss(11) //七天机器离线数（坏机器数量）
        } else {
          timeNum = "30" //运营天数
          machineNum = "3" //机器数量
          machineNumber = "0" //坏机器数量
        }


        val dispMap = dispBv.value
        val disp = dispMap.get(pointId)
        var productNum = ""
        //产品数量
        var vacancyRate = "" //空置率
        if (disp != null) {
          productNum = disp.split("&")(0)
          vacancyRate = disp.split("&")(1)
        }


        //创建时间,支付账号,支付时间,网点id,点位id
        //小区名,城市,小区户数,小区物业费,小区存在多少年
        //运营天数,机器数量,七天机器离线数（坏机器数量）,产品数量
        val result = foundTime + "," + paymentAccount + "," + paymentTime + "," + nettypeId + "," + pointId +
          "," + pointName + "," + city + "," + peoples + "," + money + "," + timeYear +
          "," + timeNum + "," + machineNum + "," + machineNumber + "," + productNum + "," + vacancyRate + "," + amountOfPayment + "," + netName + "," + playMoney

        list.::=(amountOfPayment, result)
      })
      list.iterator
    }).filter(x => (!x._1.equals("100.0") && (x._1.toDouble < 15000.0))) //过滤活动记录和高价值订单
      .mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row._2.toString.split(",")
        list.::=(line(4), row._2.toString)
      })
      list.iterator
    }).reduceByKey(_ + "@" + _).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val lines = row._2.toString.split("@")
        val line = lines(0).split(",")
        val arr = new Array[Long](lines.length)
        //存放购买时间搓
        var map: Map[String, String] = Map()
        //存放去重后购买总人数
        var mapNum: Map[String, String] = Map() //存放复购人数
        //val array = new ArrayBuffer[String]()

        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = new Date()

        var monerys = 0.0
        //统计七天内销售金额
        var vipPlay = 0 //vip支付方式
        for (l <- 0 until lines.length) {
          val ll = lines(l).split(",")
          arr(l) = df.parse(ll(0)).getTime
          if (!map.contains(ll(1))) {
            //去重
            map += (ll(1) -> ll(2))
          } else {
            if (!mapNum.contains(ll(1))) {
              //判断复购人数
              mapNum += (ll(1) -> ll(2))
            }
          }
          monerys += ll(15).toDouble
          if ("102".equals(ll(17))) {
            //判断是否是会员支付
            vipPlay += 1
          }
        }

        var number = 0.0
        for (m <- mapNum) {
          var mapLine: Map[String, Long] = Map()
          for (l <- 0 until lines.length) {
            val ll = lines(l).split(",")
            if (m._1.equals(ll(1))) {
              mapLine += (ll(1) -> df.parse(ll(2)).getTime)
            }
          }
          val n = timeByDifference(mapLine.min._2, mapLine.max._2)
          number += n.toDouble / mapLine.size
        }
        var vipNum = 0
        var vipOne = 0
        var vipTwo = 0
        var vipThree = 0
        var vipFour = 0
        var vipFive = 0
        val userMap = userBv.value
        for (m <- map) {
          val userNum = userMap.get(m._1)
          if (userNum != null) {
            if (!"".equals(userNum)) {
              vipNum += 1
            }
            if ("1".equals(userNum)) {
              vipOne += 1
            }
            if ("2".equals(userNum)) {
              vipTwo += 1
            }
            if ("3".equals(userNum)) {
              vipThree += 1
            }
            if ("4".equals(userNum)) {
              vipFour += 1
            }
            if ("5".equals(userNum)) {
              vipFive += 1
            }
          }
        }

        val vip = vipNum + "," + vipOne + "," + vipTwo + "," + vipThree + "," + vipFour + "," + vipFive + "," + vipPlay //VIP信息

        val purchasePeople = map.size
        //购买人数
        val repeatPeople = mapNum.size
        //复购人数
        var repeatPeopleTime = 0.0
        if (repeatPeople != 0) {
          repeatPeopleTime = number / repeatPeople //复购平均时间间隔
        }

        //数组排序
        scala.util.Sorting.quickSort(arr)

        val endTime = timeByDifference(arr.max, date.getTime) //最后一次购买距离时间间隔


        //创建时间,支付账号,支付时间,网点id,点位id
        //小区名,城市,小区户数,小区物业费,小区存在多少年
        //运营天数,机器数量,七天机器离线数（坏机器数量）,产品数量
        val foundTime = line(0)
        val paymentAccount = line(1)
        val paymentTime = line(2)
        val nettypeId = line(3)
        val pointId = line(4)
        val communityName = line(5)
        val city = line(6)
        val peoples = line(7)
        val money = line(8)
        val createTime = line(9)
        val timeNum = line(10)

        val machineNum = line(11)
        val machineNumber = line(12)

        val productNum = line(13)

        val vacancyRate = line(14)
        //空置率
        val netName = line(16)
        //网点名称
        val averageValue: Double = (monerys / machineNum.toDouble / 7) //台销

        /*
        台销（7日销量/机器数）,城市,网点id,网点名称,点位id,点位名称,机器数,
        七天机器离线数（坏机器数量）,产品数,空置率,最后一次销售距离今天的时间,
        运营时间,购买人数,复购人数,复购平均间隔,
        小区存在多少年,户数,物业费,vip人数,等级一,等级二,等级三,等级四,等级五,vip购买方式人数
         */
        val result = averageValue + "," + city + "," + nettypeId + "," + netName + "," + pointId + "," + communityName + "," + machineNum +
          "," + machineNumber + "," + productNum + "," + vacancyRate + "," + endTime +
          "," + timeNum + "," + purchasePeople + "," + repeatPeople + "," + repeatPeopleTime +
          "," + createTime + "," + peoples + "," + money + "," + vip

        list.::=(pointId, result)
      })
      list.iterator
    }).collect()

    val orderMap = new util.HashMap[String, String]
    for (o <- order) {
      orderMap.put(o._1, o._2)
    }
    log.info("clusteringOrder is end . . .")
    orderMap
  }

  //  /**
  //    * 第一次map
  //    * @param row
  //    * @param machineBv
  //    * @param dispBv
  //    * @return
  //    */
  //
  //  def orderOneMap(row:String,machineBv:Broadcast[util.HashMap[String, String]],
  //                  dispBv:Broadcast[util.HashMap[String, String]],
  //                  nettypeBv:Broadcast[util.HashMap[String, String]],sc:SparkContext): (String,String) ={
  //    val line = row.split(",")
  //    val foundTime = line(0)//创建时间
  //    val amountOfPayment = line(4)//支付金额
  //    val paymentAccount = line(5)//支付账号
  //    val paymentTime = line(6)//支付时间
  //    val nettypeId = line(8)//网点id
  //    val pointId = line(9)//点位id
  //    val playMoney = line(3)//支付方式
  //
  //
  //
  //    //id,名称,网点id,创建时间,状态(0,1),类型(点位、网点),省,市,区
  //    //地址,运营商,纬度,经度,点位序号,点位类型,覆盖户数,入住户数
  //    //电信信号强度,移动信号强度,联通信号强度,信号运营商,信号解决方案
  //
  //    val pointMap = NetTypeUtils.salesPointData(sc)//获取点位信息
  //    val pointData = pointMap.get(pointId)
  //
  //
  //    val point = pointData.split(",")
  //    val city = point(6)+"_"+point(7)//城市
  //    val createTime = point(3)//小区点位创建时间
  //    val pointName = point(1)//点位名
  //
  //
  //    //id,名称,网点id,创建时间,状态(0,1),类型(点位、网点),省,市,区    0-8
  //    //地址,运营商,纬度,经度,渠道类型id,物业id,商圈备注               9-15
  //    //入住户数,楼栋总数,地下停车场数量,地下停车场总车位数               16-19
  //    //车库电梯停总数,交房日期,物业费,楼盘价格,有无自主终端                20-24
  //    val netMap = NetTypeUtils.salesNetData(sc)
  //    val net = netMap.get(nettypeId).split(",")
  //    val netName = net(1) //网点名称
  //    val peoples = net(16)//小区户数
  //    val money = net(22)//小区物业费
  //
  //
  //    // 最早运营时间,运营天数,主机数量,机器数量,日均掉线5次以上的主机数,近7日日均掉线5次以上的主机数,近7日日均掉线20次以上的主机数,日均掉线时长超过6小时的主机数,日均掉线超过10次的主机数
  //    val machineMap = machineBv.value
  //    val machine = machineMap.get(pointId)
  //
  //    var timeNum = ""//运营天数
  //    var machineNum = ""//机器数量
  //    var machineNumber = ""//坏机器数量
  //
  //    if(machine != null){//判断根据点位是否找到对应机器信息,没有就给定默认值
  //      val sss = machine.split(",")
  //      timeNum = sss(2)//运营天数
  //      machineNum = sss(4)//机器数量
  //      machineNumber = sss(8)//坏机器数量
  //    }else{
  //      timeNum = "30"//运营天数
  //      machineNum = "3"//机器数量
  //      machineNumber = "0"//坏机器数量
  //    }
  //
  //
  //    val dispMap = dispBv.value
  //    val disp = dispMap.get(pointId)
  //    var productNum = ""//产品数量
  //    var vacancyRate = ""//空置率
  //    if(disp != null){
  //      productNum = disp.split("&")(0)
  //      vacancyRate = disp.split("&")(1)
  //    }
  //
  //
  //    //创建时间,支付账号,支付时间,网点id,点位id
  //    //小区名,城市,小区户数,小区物业费,小区点位创建时间
  //    //运营天数,机器数量,坏机器数量,产品数量
  //    val result = foundTime+","+paymentAccount+","+paymentTime+","+nettypeId+","+pointId+
  //                ","+pointName+","+city+","+peoples+","+money+","+createTime+
  //                ","+timeNum+","+machineNum+","+machineNumber+","+productNum+","+vacancyRate+","+amountOfPayment+","+netName+","+playMoney
  //
  //    (amountOfPayment,result)
  //  }
  //
  //  /**
  //    * 第二次map计算,聚合数据
  //    * @param row
  //    * @return
  //    */
  //  def orderTwoMap(row:String,
  //                  userBv:Broadcast[util.HashMap[String, String]]): (String,String) ={
  //    val lines = row.split("@")
  //    val line = lines(0).split(",")
  //    val arr = new Array[Long](lines.length)//存放购买时间搓
  //    var map:Map[String,String] = Map()//存放去重后购买总人数
  //    var mapNum:Map[String,String] = Map()//存放复购人数
  //    //val array = new ArrayBuffer[String]()
  //
  //    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //    val date = new Date()
  //
  //    var monerys = 0.0//统计七天内销售金额
  //    var vipPlay = 0//vip支付方式
  //    for(l <-  0 until lines.length){
  //      val ll = lines(l).split(",")
  //      arr(l) = df.parse(ll(0)).getTime
  //      if(!map.contains(ll(1))){//去重
  //        map += (ll(1) -> ll(2))
  //      }else{
  //        if(!mapNum.contains(ll(1))){//判断复购人数
  //          mapNum +=  (ll(1) -> ll(2))
  //        }
  //      }
  //      monerys += ll(15).toDouble
  //      if("102".equals(ll(17))){//判断是否是会员支付
  //        vipPlay += 1
  //      }
  //    }
  //
  //    var number = 0.0
  //    for(m <- mapNum){
  //      var mapLine:Map[String,Long] = Map()
  //      for(l <- 0 until lines.length ){
  //        val ll = lines(l).split(",")
  //        if(m._1.equals(ll(1))){
  //          mapLine += (ll(1) -> df.parse(ll(2)).getTime)
  //        }
  //      }
  //      val n = timeByDifference(mapLine.min._2,mapLine.max._2)
  //      number += n.toDouble / mapLine.size
  //    }
  //    var vipNum = 0
  //    var vipOne = 0
  //    var vipTwo = 0
  //    var vipThree = 0
  //    var vipFour = 0
  //    var vipFive = 0
  //    val userMap = userBv.value
  //    for(m <- map){
  //      val userNum = userMap.get(m._1)
  //      if(userNum != null){
  //        if(!"".equals(userNum)){
  //          vipNum += 1
  //        }
  //        if("1".equals(userNum)){
  //          vipOne += 1
  //        }
  //        if("2".equals(userNum)){
  //          vipTwo += 1
  //        }
  //        if("3".equals(userNum)){
  //          vipThree += 1
  //        }
  //        if("4".equals(userNum)){
  //          vipFour +=1
  //        }
  //        if("5".equals(userNum)){
  //          vipFive += 1
  //        }
  //      }
  //    }
  //
  //    val vip = vipNum+","+vipOne+","+vipTwo+","+vipThree+","+vipFour+","+vipFive+","+vipPlay//VIP信息
  //
  //    val purchasePeople = map.size//购买人数
  //    val repeatPeople = mapNum.size//复购人数
  //    var repeatPeopleTime = 0.0
  //    if(repeatPeople != 0){
  //      repeatPeopleTime = number / repeatPeople //复购平均时间间隔
  //    }
  //
  //    //数组排序
  //    scala.util.Sorting.quickSort(arr)
  //
  //    val endTime = timeByDifference(arr.max,date.getTime)//最后一次购买距离时间间隔
  //
  //
  //    //创建时间,支付账号,支付时间,网点id,点位id
  //    //小区名,城市,小区户数,小区物业费,小区点位创建时间
  //    //运营天数,机器数量,坏机器数量,产品数量
  //    val foundTime = line(0)
  //    val paymentAccount = line(1)
  //    val paymentTime = line(2)
  //    val nettypeId = line(3)
  //    val pointId = line(4)
  //    val communityName = line(5)
  //    val city = line(6)
  //    val peoples = line(7)
  //    val money = line(8)
  //    val createTime = line(9)
  //    val timeNum = line(10)
  //
  //    val machineNum = line(11)
  //    val machineNumber = line(12)
  //
  //    val productNum = line(13)
  //
  //    val vacancyRate = line(14)//空置率
  //    val netName = line(16)//网点名称
  //    val averageValue:Double = (monerys / machineNum.toDouble / 7)//台销
  //
  //    /*
  //    台销（7日销量/机器数）,城市,网点id,网点名称,点位id,机器数,
  //    坏机器数,产品数,空置率,最后一次销售距离今天的时间,
  //    运营时间,购买人数,复购人数,复购平均间隔,
  //    所在网点的小区时间,户数,物业费
  //     */
  //    val result =  averageValue + "," + city + "," +nettypeId+ ","+netName + ","+ pointId + "," + communityName + "," + machineNum +
  //              "," + machineNumber + "," + productNum + "," + vacancyRate + "," + endTime +
  //              "," + timeNum + "," + purchasePeople + "," + repeatPeople + "," + repeatPeopleTime +
  //              "," + createTime + "," + peoples + "," + money + "," + vip
  //
  //    (pointId,result)
  //  }
  //

  /**
    * 关联网点
    *
    * @param sc
    * @param path
    * @param bv
    */
  def pointCommunity(sc: SparkContext, path: String, bv: Broadcast[util.HashMap[String, String]],
                     machineBv: Broadcast[util.HashMap[String, String]],
                     dispBv: Broadcast[util.HashMap[String, String]],
                     activityUserBv: Broadcast[util.HashMap[String, String]],
                     pointDataBv: Broadcast[util.HashMap[String, String]],
                     netDataBv: Broadcast[util.HashMap[String, String]],
                     pointNumBv: Broadcast[util.HashMap[String, String]]): RDD[(String, String)] = {
    val pointCommunity = sc.textFile(path).mapPartitions(x => {
      var list = List[(String, String)]()
      x.foreach(row => {
        val line = row.toString.split(",")
        val pointId = line(0)
        val netId = line(2)
        val pointMap = pointDataBv.value
        val netMap = netDataBv.value

        val pointNumber = pointNumBv.value.get(netId) //查询网点点位数,机器数

        val point = pointMap.get(pointId)
        val net = netMap.get(netId)

        val activityUserMap = activityUserBv.value
        val map = bv.value
        val data = map.get(pointId)
        var num = activityUserMap.get(pointId)
        if (num == null) {
          //活动用户数
          num = "0"
        }
        /*
        台销（7日销量/机器数）,城市,网点id,网点名称,点位id,点位名称,机器数,
        七天机器离线数（坏机器数量）,产品数,空置率,最后一次销售距离今天的时间,
        运营时间,购买人数,复购人数,复购平均间隔,
        小区存在多少年,户数,物业费,vip人数,等级一,等级二,等级三,等级四,等级五,vip购买方式人数
        活动用户数,所属网点点位数,所属网点机器数
         */
        if (data != null) {
          //有销售记录
          list.::=(pointId, data + "," + num + "," + pointNumber)
        } else {
          //无销售记录
          list.::=(pointId, supplementData(pointId, point, machineBv, dispBv, net, pointDataBv, netDataBv) + "," + num + "," + pointNumber)
        }
      })
      list.iterator
    })
    pointCommunity
  }


  /**
    * 无销售记录
    *
    * @param pointId
    * @param point
    * @param machineBv
    * @param dispBv
    * @return
    */

  def supplementData(pointId: String, point: String, machineBv: Broadcast[util.HashMap[String, String]],
                     dispBv: Broadcast[util.HashMap[String, String]], net: String,
                     pointDataBv: Broadcast[util.HashMap[String, String]],
                     netDataBv: Broadcast[util.HashMap[String, String]]): String = {
    val machine = machineBv.value.get(pointId)
    val disp = dispBv.value.get(pointId)

    var productNum = ""
    //产品数量
    var vacancyRate = "" //空置率
    if (disp != null) {
      productNum = disp.split("&")(0)
      vacancyRate = disp.split("&")(1)
    }


    var timeNum = ""
    //运营天数
    var machineNum = ""
    //机器数量
    var machineNumber = "" //七天机器离线数（坏机器数量）

    if (machine != null) {
      //判断根据点位是否找到对应机器信息,没有就给定默认值
      val sss = machine.split(",")
      timeNum = sss(2) //运营天数
      machineNum = sss(4) //机器数量
      machineNumber = sss(11) //七天机器离线数（坏机器数量）
    } else {
      timeNum = "30" //运营天数
      machineNum = "3" //机器数量
      machineNumber = "0" //坏机器数量
    }


    var r = ""
    if (point == null) {
      println(pointId)
      r = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,seven"
    } else {
      r = point
    }

    val pointData = r.split(",")
    val pointName = pointData(1) //点位名


    var sss = net
    if (sss == null) {
      sss = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,seven"
    }
    val netData = sss.split(",")
    val netId = netData(0)
    val netName = netData(1) //网点名称
    val city = netData(6) + "_" + netData(7)
    //城市
    val peoples = netData(16)
    //小区户数
    val money = netData(22)
    //小区物业费
    val createTime = netData(21) //小区点位创建时间

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()

    var timeYear: Double = 0.0
    if (!"".equals(createTime)) {
      timeYear = (date.getTime - df.parse(createTime).getTime) / (1000 * 60 * 60 * 24L) / 365 //小区运行多少年
    }

    /*
      台销（7日销量/机器数）,城市,网点id,网点名称,点位id,机器数,
    七天机器离线数(坏机器数),产品数,最后一次销售距离今天的时间,
    运营时间,购买人数,复购人数,复购平均间隔,
    所在网点的小区时间,户数,物业费
     */

    val result = "0" + "," + city + "," + netId + "," + netName + "," + pointId + "," + pointName + "," + machineNum +
      "," + machineNumber + "," + productNum + "," + vacancyRate + "," + "" +
      "," + timeNum + "," + "" + "," + "" + "," + "" +
      "," + timeYear + "," + peoples + "," + money + ",0,0,0,0,0,0,0"

    result
  }


  /**
    * 销量好网点信息，日均销售额大于三百(15天)
    *
    * @param sc
    * @param path
    * @param rdd
    */
  def goodNetThan250(sc: SparkContext, path: String, rdd: RDD[(String, String)], newPath: String): Unit = {
    val net = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, Int]()
      x.foreach(row => {
        hashMap.put(row, 1)
      })
      hashMap.iterator
    }).collect()
    val map = new util.HashMap[String, Int]()
    for (n <- net) {
      map.put(n._1, n._2)
    }
    val data = rdd.filter(x => {
      val line = x._2.split(",")
      val netId = line(2)
      map.containsKey(netId)
    }).cache()
    data.repartition(1).saveAsTextFile(newPath)
  }

  /**
    * 销量差网点信息
    *
    * @param sc
    * @param path
    * @param rdd
    */
  def badNet(sc: SparkContext, path: String, rdd: RDD[(String, String)]): Unit = {
    val net = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, Int]()
      x.foreach(row => {
        hashMap.put(row, 1)
      })
      hashMap.iterator
    }).collect()
    val map = new util.HashMap[String, Int]()
    for (n <- net) {
      map.put(n._1, n._2)
    }
    val data = rdd.filter(x => {
      val line = x._2.split(",")
      val netId = line(2)
      map.containsKey(netId)
    }).cache()
    data.repartition(1).saveAsTextFile("/Users/seven/data/orderData/bad/")
  }

  /**
    * 变异系数好网点信息
    *
    * @param sc
    * @param path
    * @param rdd
    */
  def variationNet(sc: SparkContext, path: String, rdd: RDD[(String, String)]): Unit = {
    val net = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, Int]()
      x.foreach(row => {
        hashMap.put(row, 1)
      })
      hashMap.iterator
    }).collect()
    val map = new util.HashMap[String, Int]()
    for (n <- net) {
      map.put(n._1, n._2)
    }
    val data = rdd.filter(x => {
      val line = x._2.split(",")
      val netId = line(2)
      map.containsKey(netId)
    }).cache()
    data.repartition(1).saveAsTextFile("/Users/seven/data/orderData/variation/")
  }
}
