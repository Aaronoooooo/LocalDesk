package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.bigdata.spark.core.req.controller.HotCategorySessionAnalysisTop10Controller
import com.mobikok.dev.framework.core.TApplication
import com.mobikok.bigdata.spark.core.req.controller.HotCategorySessionAnalysisTop10Controller

object HotCategorySessionAnalysisTop10Application extends App with TApplication{

    // TODO 热门品类前10应用程序
    start("spark") {
        val controller = new HotCategorySessionAnalysisTop10Controller
        controller.execute()
    }
}
