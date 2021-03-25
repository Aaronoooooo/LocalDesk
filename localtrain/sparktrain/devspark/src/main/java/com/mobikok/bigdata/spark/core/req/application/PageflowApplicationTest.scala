package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.bigdata.spark.core.req.controller.{PageflowControllerTest}
import com.mobikok.dev.framework.core.TApplication

object PageflowApplicationTest extends App with TApplication{

    start( "spark" ) {
        val controller = new PageflowControllerTest
        controller.execute()
    }
}
