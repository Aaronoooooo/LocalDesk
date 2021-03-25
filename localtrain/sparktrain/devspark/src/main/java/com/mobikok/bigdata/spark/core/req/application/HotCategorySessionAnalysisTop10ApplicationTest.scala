package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.bigdata.spark.core.req.controller.{HotCategoryAnalysisTop10ControllerTest, HotCategorySessionAnalysisTop10ControllerTest}
import com.mobikok.dev.framework.core.TApplication

object HotCategorySessionAnalysisTop10ApplicationTest extends App with TApplication{
    start("spark"){
        val controllerTest = new HotCategorySessionAnalysisTop10ControllerTest
        controllerTest.execute()
    }
}