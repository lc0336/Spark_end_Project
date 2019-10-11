package com.cwq.canal_kafka.client

import java.net.InetSocketAddress

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}
import com.cwq.canal_kafka.utils.CanalUtil

/**
  * Canal的客户端
  */
object CanalClient {
  def main(args: Array[String]): Unit = {
    //1.创建能连接到canal的连接对象
    //hostname:canal主机，port:canal的端口
    val inetSocketAddress = new InetSocketAddress("hadoop201",11111)
    //destination：实例
    val connector: CanalConnector = CanalConnectors.newSingleConnector(inetSocketAddress,"example","","")
    //2.连接
    connector.connect()
    //3.订阅（即：监控指定的表的数据的变化）
    connector.subscribe("gmall.order_info")
    //4.循环不断的从canal中监听变化的表
    while (true) {
      // 4. 获取消息  (一个消息对应 多条sql 语句的执行)
      val msg = connector.get(100) // 一次最多获取 100 条 sql
      // 5. 一个消息对应多行数据发生了变化, 一个 entry 表示一条 sql 语句的执行
      val entries: java.util.List[CanalEntry.Entry] = msg.getEntries
      import scala.collection.JavaConversions._
      if(entries.nonEmpty){
        // 6. 遍历每行数据(查看每条SQl的执行)
        for(entry <- entries){
          // 7. EntryType.ROWDATA 只对这样的 EntryType 做处理
          if(entry.getEntryType == EntryType.ROWDATA){
            // 8. 获取到这行数据, 但是这种数据不是字符串, 所以要解析
            val value= entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(value)
            // 9.定义专门处理的工具类: 参数 1 表名, 参数 2 事件类型(插入, 删除等), 参数 3: 具体的数据
            CanalUtil.handle(entry.getHeader.getTableName, rowChange.getEventType, rowChange.getRowDatasList)
          }
        }
      }else {
      println("没有抓取到数据...., 2s 之后重新抓取")
      Thread.sleep(2000)
    }
  }
  }
}
