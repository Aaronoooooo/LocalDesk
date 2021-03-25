package com.mobikok.bigdata.spark.streaming.req.controller

import com.mobikok.bigdata.spark.streaming.req.service.LasHourAdCountAnalysisService
import com.mobikok.dev.framework.core.TController

class LasHourAdCountAnalysisController extends TController{

    private val lasHourAdCountAnalysisService = new LasHourAdCountAnalysisService

    override def execute(): Unit = {
        val result = lasHourAdCountAnalysisService.analysis()
    }
}
