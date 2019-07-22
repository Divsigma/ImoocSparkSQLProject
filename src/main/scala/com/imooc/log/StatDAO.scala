package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计DAO操作
  */
object StatDAO {

  //学习基本写法
  def insertDayCodeTopN(list: ListBuffer[DayCodeStat]): Unit ={
    //1.初始化connection,prepared statement
    var con: Connection = null
    var pstmt: PreparedStatement = null

    //2.try catch finally执行
    try {
      //2.1 通过连接工具类申请资源，获取connection
      con = MySQLUtils.getConnection()
      con.setAutoCommit(false)   //2.2.3关闭自动提交的批处理调优

      //2.2 执行SQL，写法同spark/SparkSQLThriftServerApp中

      //2.2.1 准备sql
      val sql = "insert into day_code_topn_stat(day,cms_id,sum) values(?,?,?)"

      //2.2.2 生成prepared statement
      pstmt = con.prepareStatement(sql)
      for(ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cms_id)
        pstmt.setLong(3, ele.sum)

        pstmt.addBatch() //增加到批次中
      }

      //2.2.3 提交执行，此处采用批处理调优
      //val res = pstmt.executeUpdate()
      pstmt.executeBatch()  //执行批量处理
      con.commit()          //再将处理结果手工提交

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //最后通过连接工具类释放资源
      MySQLUtils.release(con,pstmt)
    }

  }

}
