package com.seven.spark.sparksql

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * author seven
  * time   2018-05-02
  * 生成网点关系与点位关系
  */
object NetType {
  private final val log = LoggerFactory.getLogger(NetType.getClass)
  val spark = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    if (args.size > 1) {
      println(args(0) + ":" + args(1))
    }
    log.info(this.getClass.getSimpleName + " is start ! ! !")

    //加载vem_nettype表
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()
    log.info("load vem_nettype is start")
    val vemNettypePath = "/yst/sta_vem/vem_nettype/*"
    saveType(vemNettypePath)
    stopWatch.stop()
    log.info("load vem_nettype is success timeout is " + stopWatch.toString)
    stopWatch.reset()

    //写point数据
    log.info("save point data is start")
    stopWatch.start()
    val pointCommunityPath = "/yst/sta_vem/point_community/*"
    val pointPath = "/yst/seven/data/pointData/"
    writePointData(pointCommunityPath, pointPath)
    stopWatch.stop()
    log.info("save point data is success timeout is " + stopWatch.toString)
    stopWatch.reset()

    //写net数据
    log.info("save net data is start")
    stopWatch.start()
    val netCommunityPath = "/yst/sta_vem/net_community/*"
    val netPath = "/yst/seven/data/netData/"
    writeNetData(netCommunityPath, netPath)
    stopWatch.stop()
    log.info("save net data is success timeout is " + stopWatch.toString)

    spark.stop()
    log.info(this.getClass.getSimpleName + " is success ! ! !")


  }

  /**
    * 设置临时列名
    *
    * @param num
    * @return
    */
  def setListOfNames(num: Int): StructType = {
    var fieldSchema = StructType(Array(StructField("a0", StringType, true)))
    for (n <- 1 to num) {
      fieldSchema = fieldSchema.add("a" + n, StringType, true)
    }
    fieldSchema
  }

  /**
    * 加载vem_nettype表
    *
    * @param path
    */
  def saveType(path: String): Unit = {
    val t = spark.read.option("delimiter", ",").csv(path)
    val typeData = spark.createDataFrame(t.rdd, setListOfNames(29))
      .withColumnRenamed("a0", "id") //id
      .withColumnRenamed("a1", "nettypeName") //名称
      .withColumnRenamed("a2", "parentId") //父节点id，如果是点位则父节点为网点id
      .withColumnRenamed("a3", "createPerson") //创建人
      .withColumnRenamed("a4", "createDate") //创建时间
      .withColumnRenamed("a5", "updatePerson") //更新人
      .withColumnRenamed("a6", "updateDate") //更新时间
      .withColumnRenamed("a7", "isDelete") //是否删除，0正常1删除
      .withColumnRenamed("a8", "type") //类型  point、net
      .withColumnRenamed("a9", "office")
      .withColumnRenamed("a10", "operatorid")
      .withColumnRenamed("a11", "province") //省
      .withColumnRenamed("a12", "city") //市
      .withColumnRenamed("a13", "county") //区县
      .withColumnRenamed("a14", "contactMan") //联系人
      .withColumnRenamed("a15", "contactPhone") //联系方式
      .withColumnRenamed("a16", "address") //地址
      .withColumnRenamed("a17", "operators") //运营商
      .withColumnRenamed("a18", "channelTypeCode") //渠道类型字典
      .withColumnRenamed("a19", "checkStatusCode") //审核状态 0:未审核 1:审核通过 2:审核不通过 3.审核中
      .withColumnRenamed("a20", "communityId") //关联net_community,point_community
      .withColumnRenamed("a21", "lat") //纬度
      .withColumnRenamed("a22", "lng") //经度
      .withColumnRenamed("a23", "auditor") //审核人
      .withColumnRenamed("a24", "auditDate") //审核时间
      .filter("isDelete = '0'") //保留正常的信息
    typeData.cache() //持久化
    typeData.createOrReplaceTempView("vem_nettype") //创建临时表

  }

  /** *
    * 写点位信息
    *
    * @param path    点位信息
    * @param newPath 新点位信息路径
    */
  def writePointData(path: String, newPath: String): Unit = {
    val p = spark.read.option("delimiter", ",").csv(path)
    val point = spark.createDataFrame(p.rdd, setListOfNames(21))
      .withColumnRenamed("a0", "id") //id
      .withColumnRenamed("a1", "pointName") //名称
      .withColumnRenamed("a2", "nettypeId") //网点id
      .withColumnRenamed("a3", "sort") //点位序号
      .withColumnRenamed("a4", "pointType") //点位类型 0地面1地上
      .withColumnRenamed("a5", "householdCoverageNum") //覆盖户数
      .withColumnRenamed("a6", "householdOccupancyNum") //入住户数
      .withColumnRenamed("a7", "ctcSignalStrength") //电信信号强度 字典 无/1/2/3/4/5
      .withColumnRenamed("a8", "cmccSignalStrength") //移动信号强度 字典 无/1/2/3/4/5
      .withColumnRenamed("a9", "cuccSignalStrength") //联通信号强度 字典 无/1/2/3/4/5
      .withColumnRenamed("a10", "createBy") //创建人
      .withColumnRenamed("a11", "createTime") //创建时间
      .withColumnRenamed("a12", "updateBy") //更新人
      .withColumnRenamed("a13", "updateTime") //更新时间
      .withColumnRenamed("a14", "signalOperator") //信号运营商
      .withColumnRenamed("a15", "signalSolution") //信号解决方案
      .withColumnRenamed("a16", "isDelete") //是否删除   0正常1删除
      .filter("isDelete = '0'") //保留正常的信息
    point.cache()
    point.createOrReplaceTempView("point_community")

    //id，名称，网点id，创建时间，状态(0,1),类型(点位、网点)，省，市，区
    //地址，运营商，纬度，经度，点位序号，点位类型，覆盖户数，入住户数
    //电信信号强度，移动信号强度，联通信号强度，信号运营商，信号解决方案
    val sql =
    """
      |select
      |v.id,p.pointName,v.parentId,v.createDate,v.isDelete,v.type,v.province,v.city,v.county,
      |v.address,v.operators,v.lat,v.lng,p.sort,p.pointType,p.householdCoverageNum,p.householdOccupancyNum,
      |p.ctcSignalStrength,p.cmccSignalStrength,p.cuccSignalStrength,p.signalOperator,p.signalSolution
      |from point_community p
      |left join vem_nettype v
      |on v.communityId = p.id
      |where v.type = 'point' and v.parentId <> '101' and v.parentId <> '620' and v.parentId <> '7667'
    """.stripMargin

    val pointData = spark.sql(sql).cache()

    pointData.repartition(1) //文件数
      .write
      .mode(SaveMode.Overwrite) //覆盖写入
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .option("delimiter", ",") //分隔符
      .option("header", "true") //保留列头
      .csv(newPath)

  }


  /** *
    * 写网点信息
    *
    * @param path    网点信息路径
    * @param newPath 新网点信息路径
    */
  def writeNetData(path: String, newPath: String): Unit = {
    val n = spark.read.option("delimiter", ",").csv(path)
    val net = spark.createDataFrame(n.rdd, setListOfNames(29))
      .withColumnRenamed("a0", "id") //id
      .withColumnRenamed("a1", "netName") //名称
      .withColumnRenamed("a2", "nettypeId") //渠道类型id
      .withColumnRenamed("a3", "propertyManagementId") //物业id
      .withColumnRenamed("a4", "province") //省
      .withColumnRenamed("a5", "city") //市
      .withColumnRenamed("a6", "district") //区
      .withColumnRenamed("a7", "tradingAreaRemark") //商圈备注
      .withColumnRenamed("a8", "householdTotalNum") //
      .withColumnRenamed("a9", "householdCheckInNum") //入住户数
      .withColumnRenamed("a10", "buildingNum") //楼栋总数
      .withColumnRenamed("a11", "undergroundParkingNum") //地下停车场数量
      .withColumnRenamed("a12", "undergroundCarparkNum") //地下停车场总车位数
      .withColumnRenamed("a13", "elevatorHallNum") //车库电梯停总数
      .withColumnRenamed("a14", "houseCompletionTime") //交房日期
      .withColumnRenamed("a15", "propertyCosts") //物业费
      .withColumnRenamed("a16", "estatePrice") //楼盘价格
      .withColumnRenamed("a17", "selfServiceTerminal") //有无自主终端
      .withColumnRenamed("a18", "explanation") //补充说明
      .withColumnRenamed("a19", "createBy") //创建人
      .withColumnRenamed("a20", "createTime") //创建时间
      .withColumnRenamed("a21", "updateBy") //修改人
      .withColumnRenamed("a22", "updateTime") //修改时间
      .withColumnRenamed("a23", "address") //地址
      .withColumnRenamed("a24", "isDelete") //是否删除 0正常1删除
      .filter("isDelete = '0'") //保留正常的信息

    net.cache()
    net.createOrReplaceTempView("net_community")


    //id，名称，网点id，创建时间，状态(0,1),类型(点位、网点)，省，市，区   0-8
    //地址，运营商，纬度，经度，渠道类型id，物业id，商圈备注              9-15
    //入住户数，楼栋总数，地下停车场数量，地下停车场总车位数       16-19
    //车库电梯停总数,交房日期,物业费,楼盘价格,有无自主终端,小区户数               20-25
    val sql =
    """
      |select
      |v.id,n.netName,v.parentId,v.createDate,v.isDelete,v.type,v.province,v.city,v.county,
      |v.address,v.operators,v.lat,v.lng,n.nettypeId,n.propertyManagementId,n.tradingAreaRemark,
      |n.householdCheckInNum,n.buildingNum,n.undergroundParkingNum,n.undergroundCarparkNum,
      |n.elevatorHallNum,n.houseCompletionTime,n.propertyCosts,n.estatePrice,selfServiceTerminal,n.householdTotalNum
      |from net_community n
      |left join vem_nettype v
      |on v.communityId = n.id
      |where v.type = 'net' and v.id <> '101' and v.id <> '7667' and v.id <> '620'
    """.stripMargin

    val netData = spark.sql(sql).cache()

    netData.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .option("delimiter", ",")
      .option("header", "true")
      .csv(newPath)
  }


}
