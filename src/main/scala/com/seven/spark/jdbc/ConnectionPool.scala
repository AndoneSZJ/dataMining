package com.seven.spark.jdbc

import java.sql.{Connection, DriverManager}
import java.util


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
  * date     2018/5/10 上午9:20
  *
  * 简易数据库连接池
  * mysql
  */
object ConnectionPool {
  private var current_num = 0 //当前连接池已产生的连接数
  private val connections = new util.LinkedList[Connection]() //连接池

  try {
    Class.forName("com.mysql.jdbc.Driver")
  } catch {
    case e: ClassCastException => println(e.toString)
  }

  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb", "root", "123456")
    conn
  }

  /**
    * 初始化连接池
    */
  private def initConnectionPool(): util.LinkedList[Connection] = {
    AnyRef.synchronized({
      if (connections.isEmpty()) {
        for (i <- 1 until 10) {
          connections.push(initConn())
          current_num += 1
          println(i)
        }
      }
      connections
    })
  }

  /**
    * 获得连接
    */
  def getConn(): Connection = {
    initConnectionPool()
    connections.poll()
  }

  /**
    * 释放连接
    */
  def releaseCon(con: Connection) {
    connections.push(con)
  }

}
