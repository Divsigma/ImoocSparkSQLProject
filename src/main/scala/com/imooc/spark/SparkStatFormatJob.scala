package com.imooc.spark

import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    //1.读取文件，返回RDD
    val log = spark.sparkContext.textFile("file:///ScalaProject/data/10000_access.log")

    //2.提取RDD前10条分析结构
    //log.take(10).foreach(println)

    //3.用map解析处理，一般都需要拼接字符串，写出
    /**
      * 1. map后跟.take(10).foreach(println)做输出测试，调整格式
      * 2. map后跟.saveAsTextFile(<folder_path>)输出清洗后的数据
      */
    log.map(row => {
      val splits = row.split(" ")
      val ip = splits(0)
      /**
        * 通过 断点 和 Debug，可知道字段3、4为访问时间信息，进行提取拼接
        * 同时要格式转化，采用自定义时间解析类
        * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
        */
      val time = splits(3) + " " + splits(4)  //注意空格，还原原格式

      /**
        * 同上思路，清洗需要多次debug，用元组测试输出看结果
        */
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)

      //元组测试
      //(ip, DateUtils.parseTime(time), url, traffic)

      DateUtils.parseTime(time) + "\t" + url + "\t" + traffic + "\t" + ip

    }).filter(row => row.split("\t")(1) != "-").saveAsTextFile("file:///ScalaProject/data/output/")


    spark.stop()

  }

}
