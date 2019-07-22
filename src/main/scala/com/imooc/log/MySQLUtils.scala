package com.imooc.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL操作工具类
  */
object MySQLUtils {

  /**
    * 获取连接
    */
  def getConnection(): Connection = {
    //也可以都在url里，底层就是这样实现的
    //DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=root")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project","root","root")
  }

  /**
    * 释放资源
    */
  def release(con: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      con.close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
