package com.zongteng.ztetl.util

import java.util.{Calendar, Date, Locale}
import java.text.SimpleDateFormat

import org.apache.commons.lang.time.FastDateFormat


/**
 * 
 * 日期处理公共接口
 * @author panmingwen  
 *
 */
object DateUtil {
  
  
  
  def getNow():String={
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      return dateFormat.format(new Date());
  }


  def format(date:Date,format:String):String={
    val dateFormat = new SimpleDateFormat(format);
    return dateFormat.format(date);
  }
  
  
  def getTime(strTime:String):Date={
    var  format="";
    
   // System.out.println("pmw"+strTime);
    var str_time=strTime;

    if(strTime.length()==10)
         format="yyyy-MM-dd";
    else
    {
        str_time=strTime.substring(0,strTime.size-2);
        format="yyyy-MM-dd HH:mm:ss";
    }
    return getTime(str_time, format);
  }
  
  
  def getTime(strTime:String,format:String):Date={
    val dateFormat = new SimpleDateFormat(format);
      return dateFormat.parse(strTime);
  }

  //获取一周之前的时间
  def getWeekBeforeTime():String={
    //val dateFormat = new SimpleDateFormat("yyyyMMdd")
    var dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE,-7)
    val weekBefore: String = dateFormat.format(cal.getTime)
    return weekBefore
  }

  /**
    * 获取当前时间，格式：yyyyMMdd（20190901）
    * @return
    */
  def getNowTime():String={
    //val dateFormat = new SimpleDateFormat("yyyyMMdd")
    var dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val nowDate: String = dateFormat.format(new Date())
    return nowDate
  }
  /**
    * 获取当前时间，格式：yyyyMMdd（20190901）
    * @return
    */
  def getNowTimess():String={
    //val dateFormat = new SimpleDateFormat("yyyyMMdd")
    var dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")
    val nowDate: String = dateFormat.format(new Date())
    return nowDate
  }

  /**
    * 解析mongodb时间显示为零时区时间,需要转换为北京时间
    * @param time
    * @return
    */
  def getBeijingTimes(time:String):String={
    var str=""
    if(time==null || "".equalsIgnoreCase(time)){
    }else{
      val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val date: Date = format.parse(time)
      val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

      val cal: Calendar = Calendar.getInstance()
      cal.setTime(date)
      cal.add(Calendar.HOUR,8)
      str=dateFormat.format(cal.getTime)
    }
    str
  }
  /**
    * 解析mongodb全量,时间显示为"Wed Jul 04 00:20:02 CST 2018",需要转换
    * @param time
    * @return
    */
  def getEnglishTimes(time:String):String={
    var str=""
    if(time==null || "".equalsIgnoreCase(time)){
    }else{
      val format: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss 'CST' yyyy", Locale.ENGLISH)
      val date: Date = format.parse(time)
      val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

      str=dateFormat.format(date)
    }
    str
  }

  
  }