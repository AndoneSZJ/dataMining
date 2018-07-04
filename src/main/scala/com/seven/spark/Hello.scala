package com.seven.spark

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
object Hello {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println("hello")
    val ss = "adfad()#qag#gsag(faf)"
    println(ss.replaceAll("[()#]",""))
  }
}
