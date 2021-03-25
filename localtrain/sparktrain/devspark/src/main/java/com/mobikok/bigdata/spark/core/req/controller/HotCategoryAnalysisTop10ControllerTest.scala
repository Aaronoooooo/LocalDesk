package com.mobikok.bigdata.spark.core.req.controller

import com.mobikok.bigdata.spark.core.req.bean
import com.mobikok.bigdata.spark.core.req.service.{HotCategoryAnalysisTop10ServiceTest}
import com.mobikok.dev.framework.core.TController

class HotCategoryAnalysisTop10ControllerTest extends TController {
  private val hotCategoryAnalysisTop10ServiceTest = new HotCategoryAnalysisTop10ServiceTest

  override def execute(): Unit = {
    val rdd = hotCategoryAnalysisTop10ServiceTest.analysis()
    rdd.foreach(println)
  }

}