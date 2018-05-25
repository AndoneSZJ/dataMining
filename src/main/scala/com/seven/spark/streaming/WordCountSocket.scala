package com.seven.spark.streaming

import com.seven.spark.jdbc.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * author seven
  */
object WordCountSocket {
  private final val log = LoggerFactory.getLogger(WordCountSocket.getClass)
  def main(args: Array[String]): Unit = {
    log.info("Start")
    readSocket()
  }
  //统计单词数量
  def readSocket(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    // 创建一个具有两个工作线程(working thread)和批次间隔为5秒的本地 StreamingContext
    // master 需要 2 个核，以防止饥饿情况(starvation scenario)
    val ssc = new StreamingContext(conf,Seconds(5))
    // 创建一个将要连接到 hostname:port 的离散流,监听 localhost:9999
    val lines = ssc.socketTextStream("localhost",9999)
    //flatMap 是一种一对多的离散流（DStream）操作  其实本质上就是对底层的rdd进行操作
    // 将每一行拆分成单词
    val words = lines.flatMap(_.split(" "))
    // 计算每一个批次中的每一个单词;
    val pairs = words.map(word => (word,1))
    // 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
    // 注意 : 必需要触发 action（很多初学者会忘记触发action操作，导致报错：No output operations registered, so nothing to execute）
    val wordCount = pairs.reduceByKey(_+_)
    wordCount.print()
    //启动计算
    ssc.start()
    //等待计算终止
    ssc.awaitTermination()
  }


  //全局统计单词数量updateStateByKey
  def readSocketByAll(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    // 创建一个具有两个工作线程(working thread)和批次间隔为5秒的本地 StreamingContext
    // master 需要 2 个核，以防止饥饿情况(starvation scenario)
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("target/checkpoint")
    // 创建一个将要连接到 hostname:port 的离散流,监听 localhost:9999
    val lines = ssc.socketTextStream("localhost",9999)
    //flatMap 是一种一对多的离散流（DStream）操作  其实本质上就是对底层的rdd进行操作
    // 将每一行拆分成单词
    val words = lines.flatMap(_.split(" "))
    // 计算每一个批次中的每一个单词;
    val pairs = words.map(word => (word,1))
    // 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
    // 注意 : 必需要触发 action（很多初学者会忘记触发action操作，导致报错：No output operations registered, so nothing to execute）

    val wordCount = pairs.updateStateByKey((values:Seq[Int],state:Option[Int]) =>{
      var newValue = state.getOrElse(0)
      for(value <- values){
        newValue += value
      }
      Option(newValue)
    })
    wordCount.print()
    //启动计算
    ssc.start()
    //等待计算终止
    ssc.awaitTermination()
  }

  //黑名单操作
  def readSocketTransform(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    // 创建一个具有两个工作线程(working thread)和批次间隔为5秒的本地 StreamingContext
    // master 需要 2 个核，以防止饥饿情况(starvation scenario)
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("target/checkpoint")

    //创建黑名单对象
    val blackList = Array(("seven",true),("stone",true))
    val blackRdd = ssc.sparkContext.parallelize(blackList)

    // 创建一个将要连接到 hostname:port 的离散流,监听 localhost:9999
    val userLog = ssc.socketTextStream("localhost",9999)
      .mapPartitions(x =>{//获取日志信息（20180508 seven）
        var list = List[(String,String)]()
        x.foreach(row => {
          list .::= (row.split(" ")(1),row)//获取名称
        })
        list.iterator
      }).transform(x =>{//对DStream的rdd操作
      x.leftOuterJoin(blackRdd).filter(x => {//先外连接join，直接join会丢失黑名单外的数据，然后过滤
        !x._2._2.getOrElse(false)
      }).mapPartitions(x =>{
        var list = List[String]()
        x.foreach(row =>{
          list .::= (row._2._1)
        })
        list.iterator
      })
    })

    userLog.print()
    //启动计算
    ssc.start()
    //等待计算终止
    ssc.awaitTermination()
  }

  //窗口统计   每隔十秒钟统计过去六十秒中单词出现的次数
  def readSocketReduceByKeyAndWindow(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    // 创建一个具有两个工作线程(working thread)和批次间隔为5秒的本地 StreamingContext
    // master 需要 2 个核，以防止饥饿情况(starvation scenario)
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("target/checkpoint")


    // 创建一个将要连接到 hostname:port 的离散流,监听 localhost:9999
    val lines = ssc.socketTextStream("localhost",9999)
    //flatMap 是一种一对多的离散流（DStream）操作  其实本质上就是对底层的rdd进行操作
    // 将每一行拆分成单词
    val words = lines.flatMap(_.split(" ")).mapPartitions(x =>{
      var list = List[(String,Int)]()
      x.foreach(row =>{
        list .::= (row,1)
      })
      list.iterator
    })

    val wordReduceByKeyAndWindow = words.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2,Seconds(60),Seconds(10))//统计时常，窗口时间

    wordReduceByKeyAndWindow.print()
    //启动计算
    ssc.start()
    //等待计算终止
    ssc.awaitTermination()
  }

  //存入mysql
  //全局统计单词数量updateStateByKey
  def readSocketByAllWriteMysql(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    // 创建一个具有两个工作线程(working thread)和批次间隔为5秒的本地 StreamingContext
    // master 需要 2 个核，以防止饥饿情况(starvation scenario)
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("target/checkpoint")
    // 创建一个将要连接到 hostname:port 的离散流,监听 localhost:9999
    val lines = ssc.socketTextStream("localhost",9999)
    //flatMap 是一种一对多的离散流（DStream）操作  其实本质上就是对底层的rdd进行操作
    // 将每一行拆分成单词
    val words = lines.flatMap(_.split(" "))
    // 计算每一个批次中的每一个单词;
    val pairs = words.map(word => (word,1))
    // 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
    // 注意 : 必需要触发 action（很多初学者会忘记触发action操作，导致报错：No output operations registered, so nothing to execute）

    val wordCount = pairs.updateStateByKey((values:Seq[Int],state:Option[Int]) =>{
      var newValue = state.getOrElse(0)
      for(value <- values){
        newValue += value
      }
      Option(newValue)
    })

    wordCount.foreachRDD(x =>{
      x.foreachPartition(xx =>{
        val conn = ConnectionPool.getConn()//从连接池得到一个数据库链接
        xx.foreach(row =>{
          val sql = "insert into wordcount(word,count) values('" + row._1 + "'," + row._2 + ")"
          val stmt = conn.createStatement()
          stmt.execute(sql)
        })
        ConnectionPool.releaseCon(conn)//返还连接对象
      })
    })
//    wordCount.print()
    //启动计算
    ssc.start()
    //等待计算终止
    ssc.awaitTermination()
  }


  def readFile(): Unit ={
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    // 创建一个具有两个工作线程(working thread)和批次间隔为5秒的本地 StreamingContext
    // master 需要 2 个核，以防止饥饿情况(starvation scenario)
    val ssc = new StreamingContext(conf,Seconds(5))
    //监听本地文件目录/hdfs目录   文件必须是移动或者重命名方式,只读取一次，以后改变叶不会在读取
    val lines = ssc.textFileStream("/home/seven/test")
    val words = lines.flatMap(_.split(" "))
    val paris = words.map(word => (word,1))
    val wordCount = paris.reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
