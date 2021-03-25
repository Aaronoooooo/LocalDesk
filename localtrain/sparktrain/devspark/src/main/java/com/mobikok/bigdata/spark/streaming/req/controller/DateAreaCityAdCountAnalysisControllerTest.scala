package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.DateAreaCityAdCountAnalysisServiceTest
import com.mobikok.dev.framework.core.TController

/**
  * @author Aaron
  * @date 2020/07/29
  */
class DateAreaCityAdCountAnalysisControllerTest extends TController{
private val dateAreaCityAdCountAnalysisServiceTest = new DateAreaCityAdCountAnalysisServiceTest
  override def execute(): Unit = {
    dateAreaCityAdCountAnalysisServiceTest.analysis()
  }
}
