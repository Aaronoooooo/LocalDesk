package com.fwmagic.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWWC {

  def main(args: Array[String]): Unit = {
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val port: Int = parameterTool.getInt("port",9999)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("localhost",port)

   /* val wc: DataStream[WordCount] = text.flatMap(t => t.split(","))
      .map(w => WordCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
        .reduce((a,b) => WordCount(a.word,a.count+b.count))
      //.sum("count")

    wc.print().setParallelism(1)*/

    env.execute("word count streaming !")
  }
}
case class WordCount(word:String,count:Long)
