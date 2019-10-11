package com.cwq.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.cwq.Table.{RedisKeyTable, TopicTable}
import com.cwq.realtime.bean.StartupLog
import com.cwq.realtime.util.{MyKafkaUtil, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  * DAU:日活
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Dau").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    val map: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getValueByKey("config.properties","kafka.broker.list"),
      ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getValueByKey("config.properties","kafka.group")
    )
    val ids: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,TopicTable.TOPIC_STARTUP)

    //将kafka中的数据映射成StartupLog对象
    val ds_obj: DStream[StartupLog] = ids.map({ case (_, value) => {
        val log: StartupLog = JSON.parseObject(value, classOf[StartupLog])
        val date = new Date()
        log.logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
        log.logHour = new SimpleDateFormat("HH").format(date)
        log
      }
    })

    //从redis中查询今日已经登录过的用户
    val filterds: DStream[StartupLog] = ds_obj.transform(rdd => {
      //获取连接
      val client: Jedis = RedisUtil.getJedisClient
      //获取今日已登录的用户
      val uidSet = client.smembers(RedisKeyTable.REDIS_DAU_KEY + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      //广播变量
      val uidSetBC = ssc.sparkContext.broadcast(uidSet)
      client.close()

      rdd.filter(startupLog => {
        val uids = uidSetBC.value
        !uids.contains(startupLog.uid) // 返回没有写过的
      })
    })

    //针对于同一批次有相同uid的情况，取ts最小的作为启动日志
    val resultDs: DStream[StartupLog] = filterds.map(log =>(log.uid,log)).groupByKey().map({case (_,it) => it.minBy(_.ts)})
    //将今日未登录的用户的启动日志写入Hase、Redis
    resultDs.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        // redis客户端
        val client: Jedis = RedisUtil.getJedisClient
        val startupLogList = it.toList
        startupLogList.foreach(startupLog => {
          println("**********" + startupLog.uid)
          // 写入到redis的set中
          client.sadd(RedisKeyTable.REDIS_DAU_KEY  + ":" + startupLog.logDate, startupLog.uid)
          //存到Hbase中
        })
        client.close()
      })
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop200,hadoop201,hadoop202:2181"))
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
