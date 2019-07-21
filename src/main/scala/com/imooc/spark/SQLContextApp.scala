package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SQLContextApp {

  def main(args: Array[String]): Unit = {

    val path = args(0)

    // 1）创建资源
    val sparkConf = new SparkConf()
      //不setMaster会被报错Error initializing SparkContext -- a master url must be set in your configuration
      //生产中appname和master由sh脚本命令指定，故生产环境中需要注释
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 2）处理
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 3）关闭资源
    sc.stop()

  }
}
