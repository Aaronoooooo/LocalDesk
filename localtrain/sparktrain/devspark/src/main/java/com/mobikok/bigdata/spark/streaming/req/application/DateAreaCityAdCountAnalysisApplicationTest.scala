package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.DateAreaCityAdCountAnalysisControllerTest
import com.mobikok.dev.framework.core.TApplication

/**
  * @author Aaron
  * @date 2020/07/29
  */
object DateAreaCityAdCountAnalysisApplicationTest extends App with TApplication {
  start("sparkStreaming") {
    val controllerTest = new DateAreaCityAdCountAnalysisControllerTest
    controllerTest.execute()
  }

}
