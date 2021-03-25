package com.mobikok.bigdata.spark.streaming.req.application

import com.mobikok.bigdata.spark.streaming.req.controller.BlackListController
import com.mobikok.dev.framework.core.TApplication

object BlackListApplication extends App with TApplication{

    start("sparkStreaming") {
        val controller = new BlackListController
        controller.execute()
    }
}
