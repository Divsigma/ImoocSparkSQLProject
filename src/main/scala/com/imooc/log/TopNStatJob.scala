package com.imooc.log

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * TopN统计的Spark作业
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {

    //1.申请资源，取消自动检测数据类型转化
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    //2.载入清洗后数据
    val logDF = spark.read.parquet("file:///ScalaProject/data/clean")

    //logDF.printSchema()
    //logDF.show(false)

    //2.数据处理--统计code的topN并写入数据库（利用MySQLUtils获取和释放连接，通过DAO进行模型操作）
    codeTopNStat(spark, logDF)

    spark.stop()

  }

  /**
    * 最受欢迎的TopN code
    */
  def codeTopNStat(spark: SparkSession, logDF: DataFrame): Unit = {

    /**
      * 统计，方法一
      * 使用df api，两种方法可以相互检验，结果应完全一致
      */
/*    import org.apache.spark.sql.functions._  //用于count
    val codeTopNDF = logDF.filter("day == '20161110' AND cmsType == 'code'")
      .groupBy("day","cmsId").agg(count("cmsId").as("sum")).orderBy(col("sum").desc)

    codeTopNDF.show(10)*/

    /**
      * 统计，方法二
      * 使用sql，两种方法可以相互检验，结果应完全一致
      */
    logDF.createOrReplaceTempView("logs")
    //先写group by，再加过滤条件where，最后填select，可以加聚集函数
    //分行注意 + 和空格（因为是字符串拼接后执行）
    val codeTopNDF = spark.sql("select day, cmsId, count(1) as sum from logs " +
      "where day = '20161110' and cmsType = 'code' " +
      "group by day, cmsId order by sum desc")

    //codeTopNDF.show(10)


    /**
      * 入库
      * 利用MySQLUtils获取和释放连接，通过DAO进行模型操作（控制器类）
      * 此例中需要调用StatDAO类的insertDayCodeTopN方法
      */
    /*        while (partitionRecord.hasNext) {
          val row = partitionRecord.next()
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val sum = row.getAs[Long]("sum")

          list.append(DayCodeStat(day, cmsId, sum))
        }
      })
      这个也有hasNext is not a member
      */
    try {
      //因为数据是分区存储的，logDF读进来其实也是分区的信息汇总
      //import spark.implicits._ 没用
      //不转化为rdd会有编译错误：
      codeTopNDF.rdd.foreachPartition(partitionRecord => {
        //1.按照方法，生成需要传入的参数
        var list = new ListBuffer[DayCodeStat]
        //println(partitionRecord)
        partitionRecord.foreach(row => {
          //注意当前操作的对象是DF，每一行就是DF[ROW]，可以根据field指定，不用row(0)
          //注意此时是对codeTopNDF进行了操作，field字段对应上面sql语句中select出来的字段！
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val sum = row.getAs[Long]("sum")

          list.append(DayCodeStat(day,cmsId,sum))
        })
        //2.对每个分区的数据，调用DAO类方法
        StatDAO.insertDayCodeTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }//不用finally，资源释放都在DAO的方法里完成了

  }

}
