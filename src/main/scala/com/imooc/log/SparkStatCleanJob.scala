package com.imooc.log

import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用spark完成数据清洗
  */
object SparkStatCleanJob {

  case class Log(date: String, url: String, traffic: Int, ip: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    //1.用RDD读取一次清洗后数据（以tab为分隔符），再处理
    val rdd = spark.sparkContext.textFile("file:///ScalaProject/data/access.log")
    //rdd.take(10).foreach(println)  //测试输出


    //2.按照DataFrameRDDApp中做法，将RDD转为DF，因为要按需求转化信息，所以最好用编程模式转化（而非case class）
    /*    import spark.implicits._
          val logDF = rdd.map(_.split("\t")).map(row => Log(row(0), row(1), row(2).toInt, row(3))).toDF
          logDF.show()   */
    val logDF = spark.createDataFrame(rdd.map(row => AccessConvertUtil.parseLog(row)), AccessConvertUtil.schema)

    //logDF.printSchema()   //测试用
    //logDF.show()  //测试用

    //3.写入到磁盘，注意按字段分区、分区个数（此例中按照天），注意目录复写模式
    //  调优点！！注意学习
    logDF.coalesce(1).write.format("parquet").partitionBy("day")
      .mode(SaveMode.Overwrite).save("file:///ScalaProject/data/clean")

    spark.stop()

  }

}
