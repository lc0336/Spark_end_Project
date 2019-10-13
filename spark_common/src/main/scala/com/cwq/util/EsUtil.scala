package com.cwq.util

import java.util

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index, Search, SearchResult}

object EsUtil {
  val esUrl = "http://hadoop200:9200"//ES服务器
  val factory = new JestClientFactory
  val conf: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
    .multiThreaded(true)//是否支持多线程
    .maxTotalConnection(20)//最大连接数
    .connTimeout(10000) //连接超时时间
    .readTimeout(10000) //读取数据时超时时间
    .build()
  factory.setHttpClientConfig(conf)

  // 获取客户端
  def getESClient = factory.getObject

  /**
    * 向ES中插入单条数据
    * @param indexName ：表名
    * @param data ：数据
    */
  def insertSingleData(indexName: String,data: Any) ={
    val client: JestClient = getESClient
    val index = new Index.Builder(data)
      .index(indexName)
      .`type`("_doc")
      .build()
    client.execute(index)
    client.close()
  }

  /**
    * 批量向ES插入数据
    * @param indexName
    * @param sources
    */
  def insertList(indexName:String,sources:Iterator[Any])={
    if(sources.nonEmpty){
      val client: JestClient = getESClient
      val bulkBuilder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType("_doc")

      sources.foreach { // 把所有的source变成action添加buck中
        //传入的是值是元组, 第一个表示id
        case (id: String, data) => bulkBuilder.addAction(new Index.Builder(data).id(id).build())
        // 其他类型 没有id, 将来省的数据会自动生成默认id
        case data => bulkBuilder.addAction(new Index.Builder(data).build())
      }
      client.execute(bulkBuilder.build())
      client.close()
    }
  }

  /**
    * 从es中查询数据
    * @param indexName
    * @param esSql
    */
  def selectAllByIndexName (indexName:String,esSql:String)={
    val client: JestClient = getESClient
    val builder = new Search.Builder(esSql).addIndex(indexName).addType("_doc").build()
    val result: SearchResult = client.execute(builder)
    println(result)
    // 1. 获取到总的document的数量
    result.getTotal
    println("*****"+result.getTotal)
    import scala.collection.JavaConversions._  // 要是使用 scala 的遍历凡是, 需要隐式转换
    val hits: util.List[SearchResult#Hit[util.HashMap[String, Any], Void]] = result.getHits(classOf[util.HashMap[String, Any]])
    for (hit <- hits) {//hits是document集合
      val source = hit.source //一条document
      for(ele<-source){//ele document中的一个field (key-value)
        println(ele)
      }
    }
  }




  def main(args: Array[String]): Unit = {
    //单条写入测试
   // insertSingleData("test",Student("zhangsan",10))
    //批量写入测试
//    val students = List(Student("lisi",18),Student("wangwu",20))
//    insertList("test",students.iterator)
    //查询测试
    var essql =
      """
        |{
        |  "query": {
        |    "match_all": {}
        |  }
        |}
      """.stripMargin
    selectAllByIndexName("test",essql)









  }

}

case class Student(name:String,age:Int)

