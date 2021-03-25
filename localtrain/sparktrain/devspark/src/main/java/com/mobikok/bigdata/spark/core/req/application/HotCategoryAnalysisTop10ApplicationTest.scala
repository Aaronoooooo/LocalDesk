package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.bigdata.spark.core.req.controller.{HotCategoryAnalysisTop10ControllerTest}
import com.mobikok.dev.framework.core.TApplication

object HotCategoryAnalysisTop10ApplicationTest extends App with TApplication{
    start("spark"){
        val controllerTest = new HotCategoryAnalysisTop10ControllerTest
        controllerTest.execute()
    }
}