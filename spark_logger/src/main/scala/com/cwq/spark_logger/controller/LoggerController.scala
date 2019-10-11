package com.cwq.spark_logger.controller

import com.cwq.spark_logger.util.{JsonUtil, MyKafkaUtil}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{PostMapping, ResponseBody}

@Controller
class LoggerController {

  private val logger = LoggerFactory.getLogger(classOf[LoggerController])

  @Autowired
  private val MyKafkaUtil = new MyKafkaUtil()

  @PostMapping(Array("/log"))
  @ResponseBody
  def getLog(log:String)={
    //将日志落盘
    val logstr = JsonUtil.addTS(log)
    logger.info(logstr)
    //将日志写入kafka
    MyKafkaUtil.sendToKafka(logstr)
    "success"
  }

}
