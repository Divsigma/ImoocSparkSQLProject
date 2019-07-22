package com.imooc.spark

import org.apache.spark.sql.SparkSession

object DataFrameCase {

  def main(args: Array[String]): Unit = {

    //申请资源
    //在spark-shell调试不需要这个
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    //读取处理
    val rdd = spark.sparkContext.textFile("D:\\ScalaProject\\data\\student.txt")

    import spark.implicits._
    //注意 | 转移，因为split中为regx？
    val df = rdd.map(_.split("\\|")).map(row => Student(row(0).toInt, row(1), row(2), row(3))).toDF()

    df.show(3,false) //是否截取
    df.take(5).foreach(println)
    df.head(3)

    df.select("name","email").filter("name='NULL' OR name=''").show()

    df.select(df("id").as("stuNo"), df("name")).show()

    df.sort(df("name").desc).show()

    //注意join，默认为inner-join，需要 ===
    val df2 = rdd.map(_.split("\\|")).map(row => Student(row(0).toInt, row(1), row(2), row(3))).toDF()
    df.join(df2.select("id","phone"), df("id") === df2("id"), "left").show()

    //撤销资源，习惯
    spark.stop()

  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
