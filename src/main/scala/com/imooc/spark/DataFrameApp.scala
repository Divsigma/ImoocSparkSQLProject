package com.imooc.spark

import org.apache.spark.sql.SparkSession

object DataFrameApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    val df = spark.read.json("D:\\ScalaProject\\data\\people.json")
    //is equivalent to
    // spark.read.format("json").load(path)

    df.printSchema()

    df.show()

    //操作可以传入字符串，也可以传入column对象
    df.select(df("name"), df("age") + 100).show()

    df.filter("age > 15").show()

    df.groupBy(df("age")).count().show()

    spark.stop()
  }

}
