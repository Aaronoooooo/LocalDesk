package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service.DateAreaCityCountAnalysisService
import com.atguigu.summer.framework.core.TController

/**
 * * @create 2020-06-16 8:57
 */
class DateAreaCityCountAnalysisController extends TController{

  private val dateAreaCityCountAnalysisService = new DateAreaCityCountAnalysisService

  override def execute(): Unit = {
    val result: Any = dateAreaCityCountAnalysisService.analysis()
  }
}
