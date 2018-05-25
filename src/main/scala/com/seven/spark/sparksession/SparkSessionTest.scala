package com.seven.spark.sparksession

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * author seven
  * time   2018-05-14
  */
object SparkSessionTest {
  val spark:SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
//    .config("spark.shuffle.sort.bypassMergeThreshold", "310")
//    .config("spark.sql.shuffle.partitions", "300")
//    .config("spark.default.parallelism", "300")
//    .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1024")
//    .config("spark.shuffle.consolidateFiles", "true")
//    .config("spark.hadoop.parquet.metadata.read.parallelism", "20")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()//创建

  def main(args: Array[String]): Unit = {
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order/*"

    val ss = spark.read.option("delimiter",",").csv(path)

    val s = spark.createDataFrame(ss.rdd,setListOfNamesBySession(18))

    s.createOrReplaceTempView("aaa")

    spark.sql("select * from aaa where a8 = 36465").show(100)
  }

  /**
    * 设置临时列名
    * @param num
    * @return
    */
  def setListOfNamesBySession(num:Int): StructType ={
    var fieldSchema = StructType(Array(StructField("a0", StringType, true)))
    for(n <- 1 to num){
      fieldSchema = fieldSchema.add("a"+n,StringType,true)
    }
    fieldSchema
  }
}
