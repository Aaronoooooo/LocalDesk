package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.MockDataController
import com.mobikok.dev.framework.core.TApplication

object MockDataApplication extends App with TApplication{

    start("sparkStreaming") {
        val controller = new MockDataController
        controller.execute()
    }
}
