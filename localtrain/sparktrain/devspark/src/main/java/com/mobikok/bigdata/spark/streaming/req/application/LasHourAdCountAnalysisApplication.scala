package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.LasHourAdCountAnalysisController
import com.mobikok.dev.framework.core.TApplication

object LasHourAdCountAnalysisApplication extends App with TApplication{

    start( "sparkStreaming" ) {
        val controller = new LasHourAdCountAnalysisController
        controller.execute()
    }
}
