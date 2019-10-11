package com.cwq.spark_interface.server;

import java.util.Map;

public interface DauServer {
    /**
     * 根据日期获取日活
     * @param date
     * @return
     */
    Long getDauByDate(String date);

    /**
     * 根据日期获取当日每小时的日活
     * @param date
     * @return
     */
    Map<String,Long> getTotalByHour(String date);
}
