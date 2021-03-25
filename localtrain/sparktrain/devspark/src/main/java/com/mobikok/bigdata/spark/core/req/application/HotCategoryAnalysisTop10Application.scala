package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.bigdata.spark.core.req.controller.HotCategoryAnalysisTop10Controller
import com.mobikok.dev.framework.core.TApplication

object HotCategoryAnalysisTop10Application extends App with TApplication{

    // TODO 热门品类前10应用程序
    start("spark") {
        val controller = new HotCategoryAnalysisTop10Controller
        controller.execute()
    }
}
