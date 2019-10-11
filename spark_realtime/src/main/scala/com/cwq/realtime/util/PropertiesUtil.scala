package com.cwq.realtime.util

import java.io.InputStream
import java.util.Properties

import scala.collection.mutable

/**
  * 用于读取properties中的内容
  */
object PropertiesUtil {

//  private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
//  private val properties = new Properties()
//  properties.load(is)
//  def getProperty(propertyName: String): String = properties.getProperty(propertyName)
//
//  def main(args: Array[String]): Unit = {
//    println(getProperty("kafka.broker.list"))
//  }

  //用于存放Properties
  private val map: mutable.Map[String, Properties] = mutable.Map[String,Properties]()
  def getValueByKey(filename:String,key:String)={
    map.getOrElseUpdate(filename,{
      val fs: InputStream = ClassLoader.getSystemResourceAsStream(filename)
      val properties = new Properties()
      properties.load(fs)
      properties
    }).getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    println(getValueByKey("config.properties","kafka.broker.list"))
  }


}
