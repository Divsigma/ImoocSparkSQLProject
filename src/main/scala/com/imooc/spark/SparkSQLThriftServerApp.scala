package com.imooc.spark

import java.sql.DriverManager

object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {

    //采用JAVA方式编程
    //1)driver实例化
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    //2)connection实例化
    val con = DriverManager.getConnection("jdbc:hive2://192.168.94.134:10000")

    //3)执行sql语句，返回结果
    val pstmt = con.prepareStatement("select sname from student")
    val res = pstmt.executeQuery()
    var i: Int = 1
    while (res.next()) {
      println("sname:" + res.getString("sname"))
    }

    //4)关闭资源
    res.close()
    pstmt.close()
    con.close()

  }

}
