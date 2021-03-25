package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.DateAreaCityAdCountAnalysisController
import com.mobikok.dev.framework.core.TApplication

object DateAreaCityAdCountAnalysisApplication extends App with TApplication {

    start( "sparkStreaming" ) {
        val controller = new DateAreaCityAdCountAnalysisController
        controller.execute()
    }
}
