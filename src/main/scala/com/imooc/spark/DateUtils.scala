package com.imooc.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析类
  * 1.unparseable date首先检查format的pattern，长度，表示等等
  * 2.SimpleDateFormat线程不安全，转化的时间可能不对
  */
object DateUtils {

  //输入时间格式[10/Nov/2016:00:01:02 +0800]
//  val YYYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val YYYYMMDDHHMM_TIME_FORMAT:FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //目标时间格式
//  val TARGET_TIME_FORMAT = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss")
  val TARGET_TIME_FORMAT:FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

  /**
    * 解析时间为需要的类型
    * 注意返回类型！否则没有println输出
    *
    */
  def parseTime(time: String): String = {

    TARGET_TIME_FORMAT.format(new Date(getLongTime(time)))

  }

  /**
    * 获取long类型的输入日志的时间
    *
    * [10/Nov/2016:00:01:02 +0800]
    */
  def getLongTime(time: String): Long = {
    //经常有异常
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  //测试用main方法
  def main(args: Array[String]): Unit = {

    println(parseTime("[10/Nov/2016:00:01:02 +0800]"))

  }
}
