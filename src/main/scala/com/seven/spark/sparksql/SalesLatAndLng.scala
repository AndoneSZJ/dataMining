package com.seven.spark.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/17 下午4:36     
  */
object SalesLatAndLng {
  private final val log = LoggerFactory.getLogger(NetType.getClass)
  val spark = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val netData = spark.read.option("delimiter",",").csv("/Users/seven/data/aaaaa/*")
    val fieldSchema = StructType(Array(StructField("id", StringType, true)))

    spark.createDataFrame(netData.rdd,fieldSchema).createOrReplaceTempView("data")

    spark.read.option("delimiter",",").option("header","true").csv("/Users/seven/data/netData/*").createOrReplaceTempView("net")

    val sql =
      """
        |select a.id,b.netName,b.lat,b.lng,b.province,b.city,b.county from data a
        |left join net b
        |on a.id = b.id
      """.stripMargin

    val data = spark.sql(sql)

    data.repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv("/Users/seven/data/bbbb/cccc/")
  }
}
