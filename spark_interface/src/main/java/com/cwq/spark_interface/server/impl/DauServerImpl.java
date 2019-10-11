package com.cwq.spark_interface.server.impl;

import com.cwq.spark_interface.dao.DauDao;
import com.cwq.spark_interface.server.DauServer;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServerImpl implements DauServer {

    @Resource
    private DauDao dauDao;

    @Override
    public Long getDauByDate(String date) {
        return dauDao.getDauByDate(date);
    }

    @Override
    public Map<String, Long> getTotalByHour(String date) {
        List<Map> totalByHour = dauDao.getTotalByHour(date);
        Map<String,Long> map = new HashMap<>();
        for (Map map1:totalByHour) {
            String loghour = (String)map1.get("loghour");
            Long count = (Long) map1.get("total");
            map.put(loghour,count);
        }
        return map;
    }
}
