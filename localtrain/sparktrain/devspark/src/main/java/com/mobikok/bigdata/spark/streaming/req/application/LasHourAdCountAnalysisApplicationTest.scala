package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.LasHourAdCountAnalysisControllerTest
import com.mobikok.dev.framework.core.TApplication

/**
  * @author Aaron
  * @date 2020/07/29
  */
object LasHourAdCountAnalysisApplicationTest extends App with TApplication{

  private val lasHourAdCountAnalysisControllerTest = new LasHourAdCountAnalysisControllerTest
  start("sparkStreaming")(
    lasHourAdCountAnalysisControllerTest.execute()
  )

}
