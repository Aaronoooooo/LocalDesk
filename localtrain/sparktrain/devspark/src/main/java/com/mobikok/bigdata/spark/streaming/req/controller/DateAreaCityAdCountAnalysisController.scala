package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.DateAreaCityAdCountAnalysisService
import com.mobikok.dev.framework.core.TController

class DateAreaCityAdCountAnalysisController extends TController{

    private val dateAreaCityAdCountAnalysisService = new DateAreaCityAdCountAnalysisService

    override def execute(): Unit = {
        val result = dateAreaCityAdCountAnalysisService.analysis()
    }
}
