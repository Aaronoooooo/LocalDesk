package com.zongteng.ztetl.util

import com.google.gson.JsonElement
import org.apache.commons.lang3.StringUtils

object DataNullUtil {

  /**
    * 采集字段为null时，转换为字符串"\\N"
    * scala要对\进行专一，所以"\\\\N"
    */
  def nvlNull(str: String): String = {
    if (str == null) "\\N" else str.trim
  }


  /**
    * null --> ""
    * "  " --> ""
    * "" --> ""
    * 不是上面三种情况的，返回字符串本身
    *
    * @param str
    * @return
    */
  def nvlString(str: String): String = {
    if (StringUtils.isBlank(str) || "null".equalsIgnoreCase(str)) "" else str.trim
  }

  /**
    * 如果obj == null，返回""；否则，转成调用对应的toString方法返回
    *
    * @param obj
    * @return
    */
  def nvlObject(obj: Object): String = {
    if (obj == null) "" else obj.toString.trim
  }

  /**
    * null --> 0
    * "  " --> ""
    * "" --> ""
    * 不是上面三种情况的，返回字符串本身
    *
    * @param string
    * @return
    */
  def nvlInt(string: String): String = {
    if (string == null || string.equalsIgnoreCase("null")) "0" else string.trim
  }


  def nvlDateTime(dateTime: String): String = {

    if (StringUtils.isBlank(dateTime) || (dateTime.substring(0, 4).toInt == 0)) {
      "\\N"
    } else {
      dateTime
    }

  }

  def dtIsNull(dateTime: String) = {

    if (StringUtils.isBlank(dateTime) || (dateTime.substring(0, 4).toInt == 0)) {
      true
    } else {
      false
    }

  }


  def nvlDate(element: JsonElement): String = {
    var str = ""
    if (element == null || "".equalsIgnoreCase(nvl(element)) || nvl(element).substring(0, 4).toInt <= 2010) {
      str = "1900-01-01 00:00:00"
    } else {
      str = nvl(element)
    }
    str

  }

  def nvl(element: JsonElement): String = {

    if (element == null || StringUtils.isBlank(element.toString) || "".equalsIgnoreCase(element.toString.replaceAll("\"", "").trim) || "null".equalsIgnoreCase(element.toString)) "" else element.toString.replaceAll("\"", "").trim

  }


}
