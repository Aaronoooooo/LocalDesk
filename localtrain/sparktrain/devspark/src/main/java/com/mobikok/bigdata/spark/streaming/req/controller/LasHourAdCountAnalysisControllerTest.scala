package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.LasHourAdCountAnalysisServiceTest
import com.mobikok.dev.framework.core.TController

/**
  * @author Aaron
  * @date 2020/07/29
  */
class LasHourAdCountAnalysisControllerTest extends TController{
  private val lasHourAdCountAnalysisServiceTest = new LasHourAdCountAnalysisServiceTest
  override def execute() = {
    lasHourAdCountAnalysisServiceTest.analysis()
  }
}
