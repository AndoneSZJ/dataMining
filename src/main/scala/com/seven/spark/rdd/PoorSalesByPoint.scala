package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.seven.spark.hdfs.Utils
import com.seven.spark.sparksql.NetTypeUtils
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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
  * date     2018/6/8 上午10:54
  *
  * 计算无销量或者7天内销量小于40的点位
  * 点位，覆盖户数，销售额，点位数，运营时间，投放时间，缺货率，商品数，空置率
  */
object PoorSalesByPoint {
  private final val log = LoggerFactory.getLogger(this.getClass)

  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    log.info("job is start . . .")
    val stopwatch = new StopWatch()
    stopwatch.start()
//    val operatePath = "/yst/vem/operate/P/main/*"
//    val operateMap = getOperateTimeByPoint(operatePath)
//    val operateBv = sc.broadcast(operateMap)

    val operatePathByNet = "/yst/vem/operate/N/main/*"
    val netMap = getOperateTimeByNet(operatePathByNet)
    val netBv = sc.broadcast(netMap)


    val disPath = "/yst/vem/disp/point/static/main/*"
    val disMap = getDisByPoint(disPath)
    val disBv = sc.broadcast(disMap)

    val orderPath = "/yst/vem/sales/order/*"
    val rdd = getPoorSalesThan40ByPoint(orderPath)

    val netNumMap = NetTypeUtils.salesNetData(sc)
    val netNumBv = sc.broadcast(netNumMap)

    val logPath = "/yst/sta_vem/vem_onoff_line_log/*"
    val logMap = getLogByMachine(logPath)
    val logBv = sc.broadcast(logMap)

    val machinePath = "/yst/sta_vem/vem_machine/*"
    val machineMap = getMachineData(machinePath,logBv)
    val machineBv = sc.broadcast(machineMap)


    val pointPath = "/yst/seven/data/pointData/*"
    getAllPoint(pointPath,machineBv,disBv,netBv,netNumBv,rdd)

    stopwatch.stop()
    log.info("job is success . . . ")
  }

  /**
    * 获取点位运营天数,超过5天的点位
    *
    * @param path
    * @return
    */
  def getOperateTimeByPoint(path: String): util.HashMap[String, String] = {
    val operate = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replace("(", "").replace(")", "").split(",")
        //点位id
        val pointId = line(0)
        //运营时间
        val time = line(2)
        //投放天数
        val createTime = line(13)
        hashMap.put(pointId, time + "," + createTime)
      })
      hashMap.iterator
    })//.filter(x => x._2.split(",")(0).toDouble > 5)
      .collect() //选取运营时间大于5天的点位

    val operateMap = new util.HashMap[String, String]
    for (o <- operate) {
      operateMap.put(o._1, o._2)
    }
    operateMap
  }

  /**
    * 获取网点点位数
    *
    * @param path
    * @return
    */
  def getOperateTimeByNet(path: String): util.HashMap[String, String] = {
    val operate = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replace("(", "").replace(")", "").split(",")
        //网点id
        val netId = line(0)
        //点位数
        val num = line(10)
        hashMap.put(netId, num)
      })
      hashMap.iterator
    })collect()

    val operateMap = new util.HashMap[String, String]
    for (o <- operate) {
      operateMap.put(o._1, o._2)
    }
    operateMap
  }

  /**
    * 获取七天内网点的销售量
    * @param path
    * @return
    */
  def getPoorSalesThan40ByPoint(path:String): RDD[(String,Double)] ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    //7天时间搓
    val long = 1000 * 3600 * 24 * 7L
    val order = sc.textFile(path).filter(x => {
      //选取7天内的销售数据
      val line = x.split(",")
      val time = line(0)
      val money = line(4)
      !"100.0".equals(money) && df.parse(time).getTime > date.getTime - long //过滤7天以外的数据，过滤活动订单
    }).mapPartitions(x => {
      var list = List[(String, Double)]()
      x.foreach(row => {
        val line = row.split(",")
        //点位id
        val pointId = line(9)
        //网点id
        val money = line(4).toDouble / 100 //订单金额
        list.::=(pointId, money)
      })
      list.iterator
    }).reduceByKey(_ + _) //聚合所在点位7天内销售金额
      .cache()
    order
  }

  /**
    * 获取点位产品信息
    *
    * @param path
    * @return
    */
  def getDisByPoint(path: String): util.HashMap[String, String] = {
    log.info("getDispByPoint is start . . .")
    val dis = sc.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String,String]()
      x.foreach(row =>{
        val line = row.replace("(","").split(",")
        val pointId = line(2)//点位id
        val shortrt = line(7)//缺货率
        val prodct = line(3)//商品数
        val emptrt = line(8)//空置率
        hashMap.put(pointId,shortrt+","+prodct+","+emptrt)
      })
      hashMap.iterator
    }).collect()
    val disMap = new util.HashMap[String, String]()
    for (d <- dis) {
      disMap.put(d._1, d._2)
    }
    log.info("getDispByPoint is end . . .")
    disMap
  }

  /**
    * 获取机器最早日志信息
    * @param path
    * @return
    */
  def getLogByMachine(path:String): java.util.HashMap[String,String] ={
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

  /**
    * 获取机器信息
    * @param path
    * @param broadcast
    */
  def getMachineData(path:String,broadcast: Broadcast[java.util.HashMap[String,String]]): util.HashMap[String,String] ={
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
        val id = line(0)//机器id
        val pointId = line(19)//点位id
//        val netId = line(18)//网点id
        val areaId = line(17)//大区id
        val createTime = line(15)//机器投放时间
        var timeOut = ""//第一次上线时间
        var timeNum = 0.0//运营天数

        val time = map.get(id)
        if(time != null){
          timeOut = time
          timeNum = (date.getTime - df.parse(time).getTime) / 1000.0 / 3600.0 / 24.0
        }

        val createTimeNum = (date.getTime - df.parse(createTime).getTime) / 1000.0 / 3600.0 / 24.0
        //点位id，大区id，机器投放时间，投放天数，第一次上线时间，运营天数
        list .::= (pointId,areaId+","+createTime+","+createTimeNum+","+timeOut+","+timeNum)
      })
      list.iterator
    }).collect()
    val map = new util.HashMap[String,String]()
    for(m <- machine){
      map.put(m._1,m._2)
    }
    map
  }


  /**
    * 获取点位信息
    * @param path
    * @param broadcastByMachine
    * @param broadcastByDis
    * @param netBv
    * @param netNumBv
    * @param rdd
    */
  def getAllPoint(path:String,//broadcast: Broadcast[util.HashMap[String,String]],
                  broadcastByMachine: Broadcast[util.HashMap[String,String]],
                  broadcastByDis: Broadcast[util.HashMap[String,String]],
                  netBv: Broadcast[util.HashMap[String,String]],
                  netNumBv: Broadcast[util.HashMap[String,String]],
                  rdd:RDD[(String,Double)]): Unit ={
    //获取7日销量超过40的点位信息
    val goodOrder = rdd.filter(x => x._2 > 40).collect()
    val goodMap = new util.HashMap[String,Double]()
    for(g <- goodOrder){
      goodMap.put(g._1,g._2)
    }
    //获取7日销量没超过40的点位信息
    val badOrder = rdd.filter(x => x._2 <= 40).collect()
    val badMap = new util.HashMap[String,Double]()
    for(b <- badOrder){
      badMap.put(b._1,b._2)
    }
    //获取7日销量信息
    val order = rdd.collect()
    val orderMap = new util.HashMap[String,Double]()
    for(o <- order){
      orderMap.put(o._1,o._2)
    }
    //获取运营数据
//    val operateMap = broadcast.value

    val point = sc.textFile(path).filter(x =>{
      val line = x.split(",")
      val pointId = line(0)
      //过滤运营天数小于5天的点位，过滤7日销量大于40的点位
      !"id".equals(pointId) //&& !goodMap.containsKey(pointId) //&& operateMap.containsKey(pointId)
    }).mapPartitions(x =>{
      var list = List[String]()
      x.foreach(row =>{
        val line = row.split(",")
        val pointId = line(0)//点位id

        val netId = line(2)//网点id

        val net = netNumBv.value
        var houseNum = "0"//覆盖户数
        var netName = ""//小区名称

        if(net.get(netId) != null){
          houseNum = net.get(netId).split(",")(25)//覆盖户数
          netName = net.get(netId).split(",")(1)
        }

        var money = 0.0//销售额

//        val pointNum = badMap.get(pointId)
        val pointNum = orderMap.get(pointId)
        if(pointNum != null){//如果七天内有销量则去实际销量，没有则为0
          money = pointNum
        }
        var num = ""

        val pointNumber = netBv.value.get(netId)//点位数

        if(pointNumber != null){//点位数量找不到则为空
          num = pointNumber
        }
        val machineMap = broadcastByMachine.value
        val machine = machineMap.get(pointId)

        //val operate = operateMap.get(pointId)
        val disMap = broadcastByDis.value
        val dis = disMap.get(pointId)
        var str = ",,"
        if(dis != null){//如果找不到产品信息，则制空
          str = dis
        }
        var times = ",,,,"
        if(machine != null){//找不到机器信息则制空
          times = machine
        }
        //点位id,网点id,网点名称,覆盖户数,销售额,所在网点的点位数,缺货率,商品数,空置率,大区id,机器投放时间,投放天数,第一次上线运营时间,运营天数
        list .::= (pointId+","+netId+","+netName+","+houseNum+","+money+","+num+","+str+","+times)
      })
      list.iterator
    }).cache()

    Utils.saveHdfs(point,sc,"/yst/seven/data/poorSalesByPoint/",1)
  }
}
