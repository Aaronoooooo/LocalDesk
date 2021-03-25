package com.mobikok.bigdata.spark.core.req.controller

import com.mobikok.bigdata.spark.core.req.bean
import com.mobikok.bigdata.spark.core.req.service.{HotCategoryAnalysisTop10ServiceTest, HotCategorySessionAnalysisTop10ServiceTest}
import com.mobikok.dev.framework.core.TController

class HotCategorySessionAnalysisTop10ControllerTest extends TController {
  private val hotCategoryAnalysisTop10ServiceTest = new HotCategoryAnalysisTop10ServiceTest
  private val hotCategorySessionAnalysisTop10ServiceTest = new HotCategorySessionAnalysisTop10ServiceTest

  override def execute(): Unit = {
    val categoties = hotCategoryAnalysisTop10ServiceTest.analysis()
    val result = hotCategorySessionAnalysisTop10ServiceTest.analysis(categoties)
    //数据样式:(20,List((199f8e1d-db1a-4174-b0c2-ef095aaef3ee,8), (7eacf77a-c019-4072-8e09-840e5cca6569,8), (85157915-aa25-4a8d-8ca0-9da1ee67fa70,7))
    result.foreach(println)
  }

}