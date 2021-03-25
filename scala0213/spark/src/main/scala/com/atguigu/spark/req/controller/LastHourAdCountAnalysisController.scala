package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service.LastHourAdCountAnalysisService
import com.atguigu.summer.framework.core.TController

/**
 * @author zxjgreat
 * @create 2020-06-16 12:41
 */
class LastHourAdCountAnalysisController extends TController{

  private val lastHourAdCountAnalysisService = new LastHourAdCountAnalysisService
  override def execute(): Unit = {

    val result: Any = lastHourAdCountAnalysisService.analysis()
  }
}
