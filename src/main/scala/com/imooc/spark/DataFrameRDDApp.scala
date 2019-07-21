package com.imooc.spark

import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.types._

object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //方法一：通过case class反射
    //inferReflection(spark)

    //方法二：通过编程方式指定
    program(spark)

  }

  def program(spark: SparkSession): Unit = {

    val rdd = spark.sparkContext.textFile("D:\\ScalaProject\\data\\infos.txt")

    //对比inferReflection，map中的类不同了，数字对应为trim而不是toInt，因为要在下面map统一处理，全部转为了string
    val rowRDD = rdd.map(_.split(",")).map(row => Row(row(0).trim, row(1), row(2).trim))

    val schemaString = "id name age"
    val field = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(field)
    //或者更加精细地指定
/*    val schema = StructType(Array(StructField("id",IntegerType,true),
                                  StructField("name",StringType,true),
                                  StructField("age",IntegerType,true)
      )))*/

    val infoDF = spark.createDataFrame(rowRDD, schema)

    infoDF.printSchema()

    infoDF.show()

    spark.stop()

  }

  def inferReflection(spark: SparkSession): Unit = {
    val rdd = spark.sparkContext.textFile("D:\\ScalaProject\\data\\infos.txt")

    //注意导入隐式转换
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(row => Info(row(0).toInt, row(1), row(2).toInt)).toDF()

    infoDF.show()


    //转化为临时表
    infoDF.createOrReplaceTempView("info")
    spark.sql("select * from info where age > 20").map(row => row(1) + " is " + row(0) + "years old").show()

    spark.stop()
  }

  case class Info(id: Int, name: String, age: Int)

}
