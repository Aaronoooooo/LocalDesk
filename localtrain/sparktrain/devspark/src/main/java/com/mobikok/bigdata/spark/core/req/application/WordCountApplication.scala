package com.mobikok.bigdata.spark.core.req.application

import com.mobikok.dev.framework.core.TApplication
import com.mobikok.bigdata.spark.core.req.controller.WordCountController
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountApplication extends App with TApplication{

    start( "spark" ) {

        val controller = new WordCountController
        controller.execute()

    }

}
