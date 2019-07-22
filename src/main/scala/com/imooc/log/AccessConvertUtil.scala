package com.imooc.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 二次清洗：访问日志转换工具类
  * 输入 ==> 输出
  */
object AccessConvertUtil {

  //1.定义输出df的字段
  val schema = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)  //分区字段
    )
  )

  //2.按照需求，将输入rdd的每一行解析为输出格式
  //  注意函数返回类型为Row，用于spark.createDataFrame中RDD[ROW]类型参数
  def parseLog(row: String): Row = {
    // for  robustness
    try {

      val splits = row.split("\t")

      val url = splits(1)
      val ip = splits(3)
      val traffic = splits(2).toLong //注意转换

      //注意初始化与判断
      val domain = "http://www.imooc.com/"
      var cmsType = ""
      var cmsId = 0l
      if (url.indexOf(domain) != -1) {
        val cms = url.substring(url.indexOf(domain) + domain.length(), url.length)
        val cmsTypeId = cms.split("/")
        if (cmsTypeId.length > 1) {
          cmsType = cmsTypeId(0)
          cmsId = cmsTypeId(1).toLong
        }
      } else {
        cmsType = null
        cmsId = -1
      }

      //借助github上ipdatabase项目开发：https://github.com/wzhe06/ipdatabase
      var city = IpUtils.getCity(ip)

      val time = splits(0)
      val day = time.substring(0,10).replaceAll("-","")

      //通过Row类转化，注意顺序对应schema中定义
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      //注意类型也要匹配，视频中Row(0)会报错，说类型和schema中定义类型不符
      case e: Exception => Row("0","0",0l,0l,"0","0","0","0")
    }


  }

}
