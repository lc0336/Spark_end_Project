package com.cwq.spark_logger.util

import com.alibaba.fastjson.JSON
import com.cwq.Table.TopicTable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class MyKafkaUtil {
  // 使用注入的方式来实例化 KafkaTemplate. Spring boot 会自动完成// 使用注入的方式来实例化 KafkaTemplate. Spring boot 会自动完成
  @Autowired
  val kafkaTemplate: KafkaTemplate[String, String] = null

  /**
    * 发送日志到 kafka
    *
    * @param logStr
    */
   def sendToKafka(logStr: String): Unit = {
    var topicName: String = TopicTable.TOPIC_STARTUP
    val logType = JSON.parseObject(logStr).getString("logType")
    if ("event" == logType) topicName = TopicTable.TOPIC_EVENT
    kafkaTemplate.send(topicName, logStr)
  }
}
