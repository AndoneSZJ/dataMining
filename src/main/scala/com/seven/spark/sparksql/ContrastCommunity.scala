package com.seven.spark.sparksql

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory
/**
  * Created by IntelliJ IDEA.
  * __   __
  * \/---\/
  * ). .(
  * ( (") )
  * )   (
  * /     \
  * (       )``
  * ( \ /-\ / )
  * w'W   W'w
  *
  * author   seven  
  * email    sevenstone@yeah.net
  * date     2018/7/6 上午9:14
  *
  * 对比楼盘数据
  */
object ContrastCommunity {

  private final val log = LoggerFactory.getLogger(NetType.getClass)
  val spark = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    log.info("start . . .")
    val stopWatch = new StopWatch()
    stopWatch.start()
    countContrast()
    stopWatch.stop()
    log.info("success . . .")
    log.info("spend time " + stopWatch.toString + "ms")
  }

  def countContrast(): Unit = {
    val community = spark.read //获取楼盘信息
      .option("header", "true") //保留头文件
      .option("delimiter", ",") //分隔符
      .csv("/yst/seven/*") //地址
      .withColumnRenamed("NETNM", "netName") //更换列名
      .withColumnRenamed("ADDRESS", "address")
      .select("netName", "address") //指定查询列
      .distinct()
      .cache() //持久化

    community.createOrReplaceTempView("community") //注册临时表

    val net = spark.read //获取id和名称的关系
      .option("header", "true")
      .option("delimiter", ",")
      .csv("/yst/seven/data/netData/*")
      .select("id", "netName")
      .cache()

    net.createOrReplaceTempView("net_community")

    val statTest = spark.read
      .option("delimiter", ",")
      .csv("/yst/vem/sales/stat/N/main/*")
      .cache()

    val fieldSchema = StructType(Array(
      StructField("a0", StringType, true),
      StructField("a1", StringType, true),
      StructField("a2", StringType, true),
      StructField("a3", StringType, true),
      StructField("a4", StringType, true),
      StructField("a5", StringType, true),
      StructField("a6", StringType, true),
      StructField("a7", StringType, true),
      StructField("a8", StringType, true)
    ))

    spark.createDataFrame(statTest.rdd, fieldSchema).createOrReplaceTempView("statTest")

    def removeSymbol(str: String): String = {
      str.replaceAll("[()]", "")
    }

    spark.udf.register("removeSymbol", removeSymbol _) //注册自定义函数

    val stat = spark.sql("select removeSymbol(a0) as id,a1 as money from statTest").cache()

    stat.createOrReplaceTempView("stat")

    val sql =
      """
        |select s.id as id,n.netName from
        |stat s left join net_community n
        |on s.id = n.id
      """.stripMargin

    val data = spark.sql(sql)

    data.show()

    data.createOrReplaceTempView("data")

    community.show()


    val s = spark.sql("select distinct(d.netName) as statName,c.netName as dataName from data d left join community c on c.netName = d.netName").cache()

    s.show()

    s.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("/yst/seven/left/")


    val ss = spark.sql("select distinct(d.netName) as statName,c.netName as dataName from data d right join community c on c.netName = d.netName").cache()

    ss.show()


    ss.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("/yst/seven/right/")


  }


}
