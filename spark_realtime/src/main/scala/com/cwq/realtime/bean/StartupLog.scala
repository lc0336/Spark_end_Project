package com.cwq.realtime.bean

case class StartupLog(mid: String, //设备id
                      uid: String,  //用户id
                      appId: String,  //appid
                      area: String, //地区
                      os: String,
                      ch: String,
                      Type: String,
                      vs: String,
                      ts: Long,
                      var logDate: String,
                      var logHour: String)
