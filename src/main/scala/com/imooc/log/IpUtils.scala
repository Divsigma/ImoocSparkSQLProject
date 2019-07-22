package com.imooc.log

import com.ggstar.util.ip.IpHelper

/**
  * Ip解析工具类
  * NoClassDefFoundError是少了poi包，所以一定要写测试类！
  */
object IpUtils {

  def getCity(ip: String): String = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("218.75.35.226"))
  }

}
