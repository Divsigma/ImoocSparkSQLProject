package com.imooc.spark

import org.apache.spark.sql.SparkSession

object ParquetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    //事实上，不用format，load底层默认是parquet文件
    val userDF = spark.read.format("parquet").load("file:///home/hadoop/app/spark-2.4.2-bin-hadoop2.7/examples/src/main/resources/users.parquet")

    userDF.printSchema()
    userDF.show()

    userDF.select("name", "favorite_color").write.format("json").save("file:///home/hadoop/data/")

    spark.close()

  }

}
