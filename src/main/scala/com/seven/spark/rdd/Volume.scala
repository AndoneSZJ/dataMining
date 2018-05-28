package com.seven.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/** *
  * 计算台销7日，30日
  * 维度：全国，城市,网点，点位
  */
object Volume {
  private final val log = LoggerFactory.getLogger(Volume.getClass)

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName(this.getClass.getSimpleName)
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    log.info("start . . .")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    log.info("jobStartTime is " + format.format(date.getTime))
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()

    //全国
    val orderpathByA = "/yst/vem/sales/stat/A/main"
    val operatePathByA = "/yst/vem/operate/A/main"
    val writeA = "/yst/seven/data/Volume/A"
    salesOperate(operatePathByA, orderpathByA, writeA)
    //城市
    val orderpathByC = "/yst/vem/sales/stat/C/main"
    val operatePathByC = "/yst/vem/operate/C/main"
    val writeC = "/yst/seven/data/Volume/C"
    salesOperate(operatePathByC, orderpathByC, writeC)
    //网点
    val orderpathByN = "/yst/vem/sales/stat/N/main"
    val operatePathByN = "/yst/vem/operate/N/main"
    val writeN = "/yst/seven/data/Volume/N"
    salesOperate(operatePathByN, orderpathByN, writeN)
    //点位
    val orderpathByP = "/yst/vem/sales/stat/P/main"
    val operatePathByP = "/yst/vem/operate/P/main"
    val writeP = "/yst/seven/data/Volume/P"
    salesOperate(operatePathByP, orderpathByP, writeP)

    stopWatch.stop()
    log.info("job time Volume is " + stopWatch.toString)
    log.info("jobEndTime is " + format.format(date.getTime))
    log.info("job is success")
    log.info("end . . .")

  }


  def salesOperate(operatePath: String, orderPath: String, writePath: String): Unit = {
    //加载运营数据
    val operate = spark.read.option("delimiter", ",").csv(operatePath)
    //设置和转换列名
    val operateData = spark.createDataFrame(operate.rdd, setName(10))
      .select("a0", "a2", "a4") //指定查询列名
      .withColumnRenamed("a0", "id") //更改列名
      .withColumnRenamed("a2", "time")
      .withColumnRenamed("a4", "num")

    //创建临时表
    operateData.createOrReplaceTempView("operate")
    //加载订单数据
    val order = spark.read.option("delimiter", ",").csv(orderPath)
    //设置和转换列名
    val orderData = spark.createDataFrame(order.rdd, setName(35))
      .select("a0", "a1", "a12")
      .withColumnRenamed("a0", "id")
      .withColumnRenamed("a1", "num7")
      .withColumnRenamed("a12", "num30")
    //创建临时表
    orderData.createOrReplaceTempView("order")

    //注册自定义行数。计算7日台销（销量/机器数/7）
    def sale7(time: String, num: String, n: String): String = {
      var nn = 3.0
      //机器数，如果为空则初始值为3
      var number = 0.0
      //销量为空，则为0
      var timeOut = 0.0
      if (!"".equals(num)) {
        number = num.toDouble
      }
      if (!"".equals(num)) {
        try {
          nn = n.toDouble
        } catch {
          case _: java.lang.NullPointerException => return "*****"
        }
      }
      if (!"".equals(time)) {
        try {
          timeOut = time.toDouble
        } catch {
          case _: java.lang.NullPointerException => return "******"
        }
      }
      if ("".equals(time) || timeOut > 7) {
        //运营时间小于七天则取实际时间
        number / 7 / nn + ""
      } else {
        number / timeOut / nn + ""
      }
    }

    spark.udf.register("sale7", sale7 _)

    //注册
    //注册自定义行数。计算30日台销（销量/机器数/30）
    def sale30(time: String, num: String, n: String): String = {
      var nn = 3.0
      //机器数，如果为空则初始值为3
      var number = 0.0
      //销量为空，则为0
      var timeOut = 0.0
      if (!"".equals(num)) {
        number = num.toDouble
      }
      if (!"".equals(num)) {
        try {
          nn = n.toDouble
        } catch {
          case _: java.lang.NullPointerException => return "*****"
        }
      }
      if (!"".equals(time)) {
        try {
          timeOut = time.toDouble
        } catch {
          case _: java.lang.NullPointerException => return "*****"
        }
      }
      if ("".equals(time) || timeOut > 30) {
        //如果运营时间小于30天，则取实际时间
        number / 30 / nn + ""
      } else {
        number / timeOut / nn + ""
      }
    }

    spark.udf.register("sale30", sale30 _)

    //去除（
    def replaceById(id: String): String = {
      id.replace("(", "")
    }

    spark.udf.register("replaceById", replaceById _)
    //关联两张表，id相等
    val sql =
      """
        |select (replaceById(a.id)) as  id,(sale7(b.time,a.num7,b.num)) as salesVolume7,(sale30(b.time,a.num30,b.num)) as  salesVolume30
        |--a.time
        |from order a
        |left join operate b
        |on a.id = b.id
      """.stripMargin

    //计算
    val data = spark.sql(sql).cache()

    data.show()
    println(data.count())
    //写文件
    data.repartition(1).write.mode(SaveMode.Overwrite) //数量1，覆盖写入
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .option("delimiter", "\t") //.text(writePath)
      .csv(writePath)
  }

  /**
    * 设置临时列名
    *
    * @param num
    * @return
    */
  def setName(num: Int): StructType = {
    var fieldSchema = StructType(Array(StructField("a0", StringType, true)))
    for (n <- 1 to num) {
      fieldSchema = fieldSchema.add("a" + n, StringType, true)
    }
    fieldSchema
  }
}
