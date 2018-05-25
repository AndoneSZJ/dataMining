package com.seven.spark.streaming

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author seven
  * time   2018-05-10
  * sparkstreaming,sparksql
  * 实时统计过去60秒内商品分类搜索前三的商品排名，5秒计算一次，10秒更新窗口时间
  * 日志格式 用户名 商品名 商品分类
  * leo iphone mobile_phone
  */
object TopThreeHotProduct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val sqlContext = new SQLContext(ssc.sparkContext)
//    val hiveContext = new HiveContext(ssc.sparkContext)

    val dstream = ssc.socketTextStream("localhost",7777)

    val data = dstream.mapPartitions(x =>{
      var list = List[(String,Int)]()
      x.foreach(row =>{
        val line = row.split(" ")
        list .::= (line(2)+"#"+line(1),1)//商品分类，商品名，次数
      })
      list.iterator
    }).reduceByKeyAndWindow((v1:Int,v2:Int) => v1+v2,Seconds(60),Seconds(10))
      .foreachRDD(x =>{
       val rowData =  x.mapPartitions(x =>{
          var list = List[Row]()
          x.foreach(row =>{
            val line = row._1.split("#")
            list .::= (Row(line(0),line(1),row._2))//商品分类，商品名，次数
          })
          list.iterator
        })

        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("product", StringType, true),
          StructField("click_count", IntegerType, true)
        ))

        val rowDframe = sqlContext.createDataFrame(rowData,structType)

        rowDframe.createOrReplaceTempView("product_click_log")

        val sql =
          """
            |select category,product,click_count
            |from (
            |select category,product,click_count,
            |row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank from product_click_log
            |) tmp
            |where rank <= 3
          """.stripMargin

        val ss = sqlContext.sql(sql)
        ss.show()
      })

    ssc.start()
    ssc.awaitTermination()
  }

}
