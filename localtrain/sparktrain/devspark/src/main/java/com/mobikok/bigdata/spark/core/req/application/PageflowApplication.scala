package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.dev.framework.core.TApplication
import com.mobikok.bigdata.spark.core.req.controller.PageflowController

object PageflowApplication extends App with TApplication{

    start( "spark" ) {
        val controller = new PageflowController
        controller.execute()
    }
}
