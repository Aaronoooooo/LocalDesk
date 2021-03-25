package com.atguigu.spark.req.application

import com.atguigu.spark.req.controller.DateAreaCityCountAnalysisController
import com.atguigu.summer.framework.core.TApplication

/**
 * @author zxjgreat
 * @create 2020-06-16 8:56
 */
object DateAreaCityCountAnalysisApplication extends App with TApplication{

  start( "sparkStreaming" ) {
    val controller = new DateAreaCityCountAnalysisController
    controller.execute()
  }

}
