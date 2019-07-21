package com.imooc.spark

import org.apache.spark.sql.SparkSession

object MySQLApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MySQLApp").master("local[2]").getOrCreate()

    //读取--方法一
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hive").option("dbtable", "TBLS").option("user","root").option("password","root")
      .load()

    //读取--方法二
    import java.util.Properties

    val conProper = new Properties()
    conProper.put("user", "root")
    conProper.put("password", "root")
    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://localhost:3306", "hive.TBLS", conProper)


    //写入--方法一
    jdbcDF.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hive").option("dbtable", "TBLS").option("user","root").option("password","root")
      .save()

    //写入--方法二
    jdbcDF2.write.jdbc("jdbc:mysql://localhost:3306", "hive.TBLS", conProper)
  }

}
