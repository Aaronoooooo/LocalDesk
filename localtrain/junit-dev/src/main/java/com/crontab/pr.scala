package com.crontab

import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

object pr {
  def main(args: Array[String]): Unit = {
    val runnable = new Runnable {
      override def run() = {
        println("Hello !!" + new Date())
      }
    }
    val service = Executors.newSingleThreadScheduledExecutor()
    // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
      service.scheduleAtFixedRate(runnable, 0, 3, TimeUnit.SECONDS)
  }
}
