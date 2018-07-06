package com.seven.spark.sparksession

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
  * date     2018/5/14 上午10:37
  */
object SparkSessionTest {
  val spark: SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    //    .config("spark.shuffle.sort.bypassMergeThreshold", "310")
    //    .config("spark.sql.shuffle.partitions", "300")
    //    .config("spark.default.parallelism", "300")
    //    .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1024")
    //    .config("spark.shuffle.consolidateFiles", "true")
    //    .config("spark.hadoop.parquet.metadata.read.parallelism", "20")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate() //创建

  def main(args: Array[String]): Unit = {
    val path = "/yst/sta_vem/vem_machine/*"
    val ss = spark.read.option("delimiter", ",").csv(path)
    val s = spark.createDataFrame(ss.rdd, setListOfNamesBySession(30))
    s.createOrReplaceTempView("sss")
    //a21-量贩机   a5->正常  a0->id
    val sss = spark.sql("select distinct a0 from sss where a21 = '2' and a5 = '0'")
    val machinePath = "/yst/sta_vem/vem_onoff_line_log/*"
    val a = spark.read.option("delimiter",",").csv(machinePath)
    val aa = spark.createDataFrame(a.rdd,setListOfNamesBySession(4))
    aa.createOrReplaceTempView("aaa")
    //a1->机器id
    val aaa = spark.sql("select distinct a1 from aaa")
    println(aaa.count())
    println(sss.count())

  }

  /**
    * 设置临时列名
    *
    * @param num
    * @return
    */
  def setListOfNamesBySession(num: Int): StructType = {
    var fieldSchema = StructType(Array(StructField("a0", StringType, true)))
    for (n <- 1 to num) {
      fieldSchema = fieldSchema.add("a" + n, StringType, true)
    }
    fieldSchema
  }


  /**
    * 读取oracle信息
    */
  def readOracle(): Unit ={
    val url = "jdbc:oracle:thin:@10.3.13.128:1527:odsprd"
    val prop = new Properties()//设置配置文件
    prop.setProperty("user", "BISHOW")//用户
    prop.setProperty("password", "Shuaiqidehhlin1")//密码
    //配置数据库信息
    val driver = "oracle.jdbc.driver.OracleDriver"
    Class.forName(driver)
    val ss = spark.read.jdbc(url,"ODS.STA_ZFJVEM_CHANNEL_RECORD",prop)//jdbc,表名,配置文件
    ss.printSchema()
  }
}
