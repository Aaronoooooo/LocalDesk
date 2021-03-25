package com.mobikok.bigdata.spark.core.req.controller

import com.mobikok.bigdata.spark.core.req.application.WordCountApplication.envData
import com.mobikok.bigdata.spark.core.req.service.WordCountService
import com.mobikok.dev.framework.core.TController
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * WordCount控制器
  */
class WordCountController extends TController{

    private val wordCountService = new WordCountService

    override def execute(): Unit = {
        val wordCountArray: Array[(String, Int)] = wordCountService.analysis()
        println(wordCountArray.mkString(","))
    }
}
