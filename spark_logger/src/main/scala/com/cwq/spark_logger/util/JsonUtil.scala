package com.cwq.spark_logger.util

import com.alibaba.fastjson.{JSON, JSONObject}

object JsonUtil {

  /**
    * 添加时间戳
    *
    * @param log
    * @return
    */
  def addTS(log: String): String = {
    val logObject: JSONObject = JSON.parseObject(log)
    logObject.put("ts",System.currentTimeMillis())
    logObject.toString
  }
}
