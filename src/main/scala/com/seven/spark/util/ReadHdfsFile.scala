package com.seven.spark.util

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    zjshi01@mail.nfsq.com.cn 
  * date     2018/5/24 上午9:58     
  */
object ReadHdfsFile {
  private final val log = LoggerFactory.getLogger(this.getClass)

  def readFile(path: String): List[String] = {
    var list = List[String]()
    var br: BufferedReader = null
    try {
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(path), conf)
      val status = fs.listStatus(new Path(path))
      for (s <- status) {
        val inputStream = fs.open(s.getPath)
        br = new BufferedReader(new InputStreamReader(inputStream))
        var line = ""
        while (null != (line = br.readLine())) {
          list.::=(line)
        }
      }
    } catch {
      case e: IOException => e.printStackTrace()
        log.error("readFile is error [{}]", e.toString)
    } finally {
      try {
        br.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
    list
  }

  def main(args: Array[String]): Unit = {
    val path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order"
    val list = readFile(path)

    list.foreach(println)
  }

}
