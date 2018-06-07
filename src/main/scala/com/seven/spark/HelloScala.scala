package com.seven.spark

import com.seven.spark.hbase.rowkey.RowKeyGenerator
import com.seven.spark.hbase.rowkey.generator.{FileRowKeyGenerator, HashRowKeyGenerator}

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
  *
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
  * date     2018/5/16 上午10:37
  */
object HelloScala {
  def main(args: Array[String]): Unit = {
    //    println("hello scala")
    //    HelloJava.main(Array("111","111"))
    //    println("2018-04-23 19:32:08.0".substring(0,11))
    //
    //    val map = new mutable.HashMap[String,Double]()
    //
    //    map.put("a",113.0)
    //    map.put("b",114.1)
    //    map.put("c",11.3)
    //    map.put("d",1111.11)
    //    map.put("e",1123.1)
    //    map.put("f",1122.1)
    //
    //    val ss = ListMap(map.toSeq.sortBy(_._2):_*)
    //
    //    for(m <- ss){
    //      println(m._1+":"+m._2)
    //    }
    val fileRowKeyGen: RowKeyGenerator[String] = new HashRowKeyGenerator()

    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))

    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))

    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))

    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))

    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))

    println(new String(fileRowKeyGen.generate("seven")))
    println(new String(fileRowKeyGen.generate("seven")))


  }
}
