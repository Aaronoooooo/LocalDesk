package com.javatime

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date}

object TimeStampOperation {
  var executeTime: String = _
  def main(args: Array[String]): Unit = {
    val sdfDetail = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    println("当前系统时间:"+sdfDetail.format(calendar.getTime))
//    获取时间差


//    calendar.add(Calendar.DATE,0)
//    calendar.set(Calendar.DATE,1)
    calendar.roll(Calendar.DATE, false)
    executeTime = sdfDetail.format(calendar.getTime)
    println("sdfDetail :"+executeTime)
    executeTime = sdf.format(calendar.getTime) + " " + "01:05:00"
    println("sdf: " + executeTime)

    if (new Date().before(sdfDetail.parse(executeTime))){
      println("下一次定时任务执行时间:"+executeTime)
    } else {
      calendar.add(Calendar.DATE,1)
      executeTime = sdf.format(calendar.getTime) + " " + "01:05:00"
      println("下一次定时任务执行时间:"+executeTime)
    }
//    calendar.set(Calendar.HOUR_OF_DAY,-2)
//    println("Calendar.HOUR_OF_DAY -2: "+sdfDetail.format(calendar.getTime))
    calendar.set(Calendar.HOUR_OF_DAY,-1)
    println("Calendar.HOUR_OF_DAY -1: "+sdfDetail.format(calendar.getTime))
    println(calendar.get(Calendar.HOUR_OF_DAY))
//    calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - 2)

    val keyTime = sdfDetail.format(calendar.getTime)
    println("执行时间前两个小时:"+keyTime)
  }
}
