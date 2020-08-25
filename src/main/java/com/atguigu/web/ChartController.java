package com.atguigu.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@Slf4j
public class ChartController {
    // springboot web
    // 用户点选标签集合 --》 ES DSL
    // ES 查询结果 进行一个验证
    // spark etl 任务结果，redis | mysql , 如何 去设计表结构
    // echarts.js vue.js + elmentui  静态引入
    // 整体项目的一个 部署 （spark hadoop hive | es kibana mysql ） springboot jar + docker
    @RequestMapping("/tags")
    public String tags(){
        return "tags";
    }

    @RequestMapping("/")
    public String index() {
        return "index";
    }
}

