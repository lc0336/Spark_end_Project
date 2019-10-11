package com.cwq.spark_interface.controller;

import com.cwq.spark_interface.server.DauServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class DauController {

    @Autowired
    private DauServer dauServer;

    @RequestMapping("/getDau")
    @ResponseBody
    public Long getDauByDate(String date){
       return dauServer.getDauByDate(date);
    }

}
