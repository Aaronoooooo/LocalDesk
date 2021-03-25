package com.zongteng.ztetl.test

import java.time.LocalDate
import java.util.{Calendar, Date}

import com.zongteng.ztetl.util.DateUtil
import org.apache.commons.lang.time.FastDateFormat


object test {
  def main(args: Array[String]): Unit = {
    val nowDate: LocalDate = LocalDate.now()
    val date: LocalDate = nowDate.minusDays(7)
    println(date)
    val str: String =DateUtil.getNowTime()
    val day: Int = str.toInt
    println(day)

  }
  //    获取今天日期
  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
    var today = dateFormat.format( now )
    today
  }

  //    获取昨天的时间
  def getYesterday():String= {
    var dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    var cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -7)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  //    获取本周开始日期
  def getNowWeekStart():String={
    var day:String=""
    var cal:Calendar =Calendar.getInstance();
    var df =  FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    day=df.format(cal.getTime())
    day
  }

  //    获取本周末日期
  def getNowWeekEnd():String={
    var day:String=""
    var cal:Calendar =Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);//这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1)// 增加一个星期，才是我们中国人的本周日的日期
    day=df.format(cal.getTime())
    day
  }

  //    本月的第一天
  def getNowMonthStart():String= {
    var day: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    day = df.format(cal.getTime()) //本月第一天
    day
  }

  //    本月最后一天
  def getNowMonthEnd():String={
    var day:String=""
    var cal:Calendar =Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    day=df.format(cal.getTime())//本月最后一天
    day
  }

  //    将时间戳转化成日期/时间
  def DateFormat(time:String):String={
    var sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    var date:String = sdf.format(new Date((time.toLong)))
    date
  }

  //    计算时间差
  def getCoreTime(start_time:String,end_Time:String)={
    var df = FastDateFormat.getInstance("HH:mm:ss")
    val begin = start_time
    val end = end_Time
    var between = (end.toLong - begin.toLong)/1000  //转化成秒
    var minute = between % 60 //转化为分钟
    val hour = between % (60*60) //转化为小时
  }

}
