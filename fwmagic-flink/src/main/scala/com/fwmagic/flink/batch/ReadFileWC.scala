package com.fwmagic.flink.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object ReadFileWC {
  def main(args: Array[String]): Unit = {
    val inputPath = "/Users/fangwei/Desktop/test.log"
    val outPath = "/Users/fangwei/Desktop/counts.csv"

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.readTextFile(inputPath)
   /* val counts = text.flatMap(_.toLowerCase
      .split("\\s")).filter(_.nonEmpty)
      .map(a =>(a, 1))
      .groupBy(_._1)
      .reduce((x, y) => (x._1, x._2 + y._2))
    //.sum(1)
    counts.setParallelism(1).print()//writeAsCsv(outPath)*/

    //env.execute("batch word count!")


  }
}

