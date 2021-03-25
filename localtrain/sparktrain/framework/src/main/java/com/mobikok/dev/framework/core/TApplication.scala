package com.mobikok.dev.framework.core

import java.net.{ServerSocket, Socket}


import com.mobikok.dev.framework.util.{EnvUtil, PropertiesUtil}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  var envData : Any = null

  /*
      开发原则： OCP Open - Close (开闭原则)
      Open : 开发的程序代码应该对功能扩展开发
      Close : 在扩展的同时不应该对原有的代码进行修改。
   */

  /**
   * 启动应用
   * 1. 函数柯里化
   * 2. 控制抽象
   *
   * @t 参数类型：jdbc, file, hive,kafka, socket, serverSocket
   */
  // def start( t:String = "jdbc" )( op : =>Unit )( implicit time : Duration = Seconds(5) ): Unit = {
  def start( t:String = "jdbc" )( op : =>Unit ) : Unit = {

    // TODO 1. 初始化环境
    if ( t == "socket" ) {
      envData = new Socket(
        PropertiesUtil.getValue("server.host"),
        PropertiesUtil.getValue("server.port").toInt)
    } else if ( t == "serverSocket" ) {
      envData = new ServerSocket(
        PropertiesUtil.getValue("server.port").toInt
      )
    } else if ( t == "spark" ) {
      envData = EnvUtil.getEnv()
    } else if ( t == "sparkStreaming" ) {
      envData = EnvUtil.getStreamingEnv()
    }

    // TODO 2. 业务逻辑
    try {
      op
    } catch {
      case ex: Exception => println("业务执行失败 ：" + ex.getMessage)
    }

    // TODO 3. 环境关闭
    if ( t == "socket" ) {
      val socket: Socket = envData.asInstanceOf[Socket]
      if ( !socket.isClosed ) {
        socket.close()
      }
    } else if ( t == "serverSocket" ) {
      val server: ServerSocket = envData.asInstanceOf[ServerSocket]
      if ( !server.isClosed ) {
        server.close()
      }
    } else if ( t == "spark" ) {
      EnvUtil.clear()
    } else if ( t == "sparkStreaming" ) {
      val ssc: StreamingContext = envData.asInstanceOf[StreamingContext]
      ssc.start()
      ssc.awaitTermination()
    }
  }
}