package com.atguigu.spark.day10

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zxjgreat
 * @create 2020-06-13 10:31
 */
object sparkStreaming_DIY {

  def main(args: Array[String]): Unit = {

    //spark环境

    //sparkstreaming使用核数至少是2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //执行逻辑

    val ds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102",9999))
    ds.print()

    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()


  }

  //继承Receiver，定义泛型

  //Receiver的构造方法有参数，所以子类在继承时，应该传递这个参数
  //重写方法
  class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

    private var socket: Socket = _

    def receive(): Unit ={
      val reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream,
          "UDF-8"
        )
      )
      var s : String = null
      while (true){
        s = reader.readLine()
        if(s != null){

          store(s)
        }
      }
    }

    override def onStart(): Unit = {
      socket = new Socket(host, port)

      new Thread("Socket Receiver") {
        setDaemon(true)
        override def run() { receive() }
      }.start()

    }

    override def onStop(): Unit = {

      socket.close()
      socket = null
    }
  }

}
