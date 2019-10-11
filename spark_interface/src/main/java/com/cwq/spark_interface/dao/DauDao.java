package com.cwq.spark_interface.dao;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

@Mapper
public interface DauDao {
    Long getDauByDate(String date);
    List<Map> getTotalByHour(String date);
}
