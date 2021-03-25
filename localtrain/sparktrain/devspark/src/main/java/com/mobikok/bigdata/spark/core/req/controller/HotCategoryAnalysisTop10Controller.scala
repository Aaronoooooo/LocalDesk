package com.mobikok.bigdata.spark.core.req.controller

import com.mobikok.dev.framework.core.TController
import com.mobikok.bigdata.spark.core.req.service.HotCategoryAnalysisTop10Service

class HotCategoryAnalysisTop10Controller extends TController {

    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

    override def execute(): Unit = {
        val result = hotCategoryAnalysisTop10Service.analysis()

        result.foreach(println)
    }
}
