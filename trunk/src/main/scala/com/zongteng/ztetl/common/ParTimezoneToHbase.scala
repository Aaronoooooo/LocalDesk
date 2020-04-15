package com.zongteng.ztetl.common

import java.util
import java.util.Calendar

import com.zongteng.ztetl.common.ParValListToHbase.insertToHbase
import com.zongteng.ztetl.entity.common.ParTimeZone
import com.zongteng.ztetl.util.{DataNullUtil, DateUtil}
import org.apache.commons.lang.time.FastDateFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object ParTimezoneToHbase {

  private val warehouseCodeArr: Array[String] = Array[String]("DE", "FRVI", "IT", "ES", "CZ", "UK", "UKFL", "UKOB", "USEA", "USNB", "USOT", "USWE", "USSC", "AU")
  //字段值
  private var warehouse_code: String = _
  private var warehouse_name: String = _
  private var timezone_year: String = _
  private var timezone_season_type: String = _
  private var timezone_season_start: String = _
  private var timezone_season_end: String = _
  private var timezone_summer_time_dif_val: String = _
  private var timezone_winner_time_dif_val: String = _
  private var timezone_summer_number: String = _
  private var timezone_winner_number: String = _
  private var timezone_number: String = _
  private var timezone_memo: String = _

  private var row_wid: String = _

  // 最终得到的put集合
  private val putList: util.ArrayList[Put] = new java.util.ArrayList[Put]
  // hbase列族
  private val column_family: String = "timezone"
  // dw层hbase表名
  private val table_name: String = "par_timezone_var"

  private var put: Put = null

  //令时更换时间
  private var timezoneMonth_Start: String = _
  private var timezoneSunday_Start: String = _
  private var timezoneTime_Start: String = _
  private var timezoneMonth_End: String = _
  private var timezoneSunday_End: String = _
  private var timezoneTime_End: String = _


  def main(args: Array[String]): Unit = {
    getMessageInfo().foreach((parTimeZone: ParTimeZone) => {
      //获取相关数据信息
      evalInfo(parTimeZone)
      var yearNumber = 2020
      while (yearNumber < 2022) {
        timezone_year = yearNumber.toString
        timezone_season_start = getTime(yearNumber, timezoneMonth_Start, timezoneSunday_Start, timezoneTime_Start, timezone_number, timezone_season_type)
        timezone_season_end = getTime(yearNumber, timezoneMonth_End, timezoneSunday_End, timezoneTime_End, timezone_number, timezone_season_type)

        row_wid = warehouse_code.concat("_").concat(timezone_year).concat("_").concat(timezone_season_type)

        println(row_wid, warehouse_code, warehouse_name, timezone_year, timezone_season_type, timezone_season_start, timezone_season_end, timezone_summer_time_dif_val, timezone_winner_time_dif_val, timezone_summer_number, timezone_winner_number, timezone_number, timezone_memo)
        getHbasePut()
        yearNumber += 1
      }
    })

    //将数据写入hbase中,
    val num: Int = insertToHbase(table_name, putList)

    println("一共插入了" + num + "条数据")


  }

  /**
    * 维护仓库信息
    *
    * @param warehouseCode
    * @return
    */
  def getWarehouseMessageInfo(warehouseCode: String) = warehouseCode match {
    case "de" => Map[String, String](
      "warehouseCode" -> "DE",
      "warehouseName" -> "德国仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "1",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "3"
    )
    case "frvi" => Map[String, String](
      "warehouseCode" -> "FRVI",
      "warehouseName" -> "法国仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "1",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "3"
    )
    case "it" => Map[String, String](
      "warehouseCode" -> "IT",
      "warehouseName" -> "意大利仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "1",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "3"
    )
    case "es" => Map[String, String](
      "warehouseCode" -> "ES",
      "warehouseName" -> "西班牙仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "1",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "3"
    )
    case "cz" => Map[String, String](
      "warehouseCode" -> "CZ",
      "warehouseName" -> "捷克仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "1",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "3"
    )
    case "uk" => Map[String, String](
      "warehouseCode" -> "UK",
      "warehouseName" -> "英国仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "0",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "1",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "2"
    )
    case "ukfl" => Map[String, String](
      "warehouseCode" -> "UKFL",
      "warehouseName" -> "英国三号仓",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "0",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "1",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "2"
    )
    case "ukob" => Map[String, String](
      "warehouseCode" -> "UKOB",
      "warehouseName" -> "英国二号仓",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "0",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "-1",
      "timezoneTimeStart" -> "1",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "-1",
      "timezoneTimeEnd" -> "2"
    )
    case "usea" => Map[String, String](
      "warehouseCode" -> "USEA",
      "warehouseName" -> "美东仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "-5",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "2",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "11",
      "timezoneSundayEnd" -> "1",
      "timezoneTimeEnd" -> "2"
    )
    case "usnb" => Map[String, String](
      "warehouseCode" -> "USNB",
      "warehouseName" -> "美东一号仓",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "-5",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "2",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "11",
      "timezoneSundayEnd" -> "1",
      "timezoneTimeEnd" -> "2"
    )
    case "usot" => Map[String, String](
      "warehouseCode" -> "USOT",
      "warehouseName" -> "美西三号仓",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "-8",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "2",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "11",
      "timezoneSundayEnd" -> "1",
      "timezoneTimeEnd" -> "2"
    )
    case "uswe" => Map[String, String](
      "warehouseCode" -> "USWE",
      "warehouseName" -> "美西仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "-8",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "2",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "11",
      "timezoneSundayEnd" -> "1",
      "timezoneTimeEnd" -> "2"
    )
    case "ussc" => Map[String, String](
      "warehouseCode" -> "USSC",
      "warehouseName" -> "美南仓库",
      "timezoneSeasonType" -> "summer_time",
      "timezoneNumber" -> "-6",
      "timezoneMonthStart" -> "3",
      "timezoneSundayStart" -> "2",
      "timezoneTimeStart" -> "2",
      "timezoneMonthEnd" -> "11",
      "timezoneSundayEnd" -> "1",
      "timezoneTimeEnd" -> "2"
    )
    case "au" => Map[String, String](
      "warehouseCode" -> "AU",
      "warehouseName" -> "澳洲仓库",
      "timezoneSeasonType" -> "winner_time",
      "timezoneNumber" -> "10",
      "timezoneMonthStart" -> "4",
      "timezoneSundayStart" -> "1",
      "timezoneTimeStart" -> "3",
      "timezoneMonthEnd" -> "10",
      "timezoneSundayEnd" -> "1",
      "timezoneTimeEnd" -> "2"
    )

    case _ => throw new Exception(warehouseCode + "的信息不存在,请添加或者修改")
  }

  /**
    * 赋值信息
    *
    * @param parTimeZone
    */
  def evalInfo(parTimeZone: ParTimeZone) = {
    warehouse_code = parTimeZone.warehouseCode
    warehouse_name = parTimeZone.warehouseName
    timezone_season_type = parTimeZone.timezoneSeasonType

    timezone_number = parTimeZone.timezoneNumber
    timezone_winner_number = parTimeZone.timezoneNumber
    timezone_summer_number = (parTimeZone.timezoneNumber.toInt + 1).toString
    timezone_winner_time_dif_val = (parTimeZone.timezoneNumber.toInt - 8).toString
    timezone_summer_time_dif_val = (parTimeZone.timezoneNumber.toInt + 1 - 8).toString

    timezone_memo = ""

    timezoneMonth_Start = parTimeZone.timezoneMonthStart
    timezoneSunday_Start = parTimeZone.timezoneSundayStart
    timezoneTime_Start = parTimeZone.timezoneTimeStart
    timezoneMonth_End = parTimeZone.timezoneMonthEnd
    timezoneSunday_End = parTimeZone.timezoneSundayEnd
    timezoneTime_End = parTimeZone.timezoneTimeEnd
  }

  /**
    * 获取所有要加载的仓库信息
    *
    * @return
    */
  def getMessageInfo() = {
    val infos: ListBuffer[ParTimeZone] = ListBuffer[ParTimeZone]()
    warehouseCodeArr.foreach((code: String) => {
      val warehouseMessage: Map[String, String] = getWarehouseMessageInfo(code.toLowerCase)
      val warehouseCodeOp: Option[String] = warehouseMessage.get("warehouseCode")
      val warehouseNameOp: Option[String] = warehouseMessage.get("warehouseName")
      val timezoneSeasonTypeOp: Option[String] = warehouseMessage.get("timezoneSeasonType")
      val timezoneNumberOp: Option[String] = warehouseMessage.get("timezoneNumber")
      val timezoneMonthStartOp: Option[String] = warehouseMessage.get("timezoneMonthStart")
      val timezoneSundayStartOp: Option[String] = warehouseMessage.get("timezoneSundayStart")
      val timezoneTimeStartOp: Option[String] = warehouseMessage.get("timezoneTimeStart")
      val timezoneMonthEndOp: Option[String] = warehouseMessage.get("timezoneMonthEnd")
      val timezoneSundayEndOp: Option[String] = warehouseMessage.get("timezoneSundayEnd")
      val timezoneTimeEndOp: Option[String] = warehouseMessage.get("timezoneTimeEnd")


      val warehouseCode: String = DataNullUtil.nvlString(warehouseCodeOp.get)
      val warehouseName: String = DataNullUtil.nvlString(warehouseNameOp.get)
      val timezoneSeasonType: String = DataNullUtil.nvlString(timezoneSeasonTypeOp.get)
      val timezoneNumber: String = DataNullUtil.nvlString(timezoneNumberOp.get)
      val timezoneMonthStart: String = DataNullUtil.nvlString(timezoneMonthStartOp.get)
      val timezoneSundayStart: String = DataNullUtil.nvlString(timezoneSundayStartOp.get)
      val timezoneTimeStart: String = DataNullUtil.nvlString(timezoneTimeStartOp.get)
      val timezoneMonthEnd: String = DataNullUtil.nvlString(timezoneMonthEndOp.get)
      val timezoneSundayEnd: String = DataNullUtil.nvlString(timezoneSundayEndOp.get)
      val timezoneTimeEnd: String = DataNullUtil.nvlString(timezoneTimeEndOp.get)

      infos += ParTimeZone(warehouseCode, warehouseName, timezoneSeasonType, timezoneNumber, timezoneMonthStart, timezoneSundayStart, timezoneTimeStart, timezoneMonthEnd, timezoneSundayEnd, timezoneTimeEnd)

    })

    infos
  }


  /**
    * 获取某个时区每年某个月的某个周日的某点结束夏令时或冬令时的北京时间
    *
    * @param year           年
    * @param month          月
    * @param numSunday      第几个周日 -1则代表最后一个周日
    * @param time           时间
    * @param timezoneNumber 时区
    * @param seasonType     夏令时还是冬令时
    * @return
    */
  def getTime(year: Int, month: String, numSunday: String, time: String, timezoneNumber: String, seasonType: String): String = {
    val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    val cal: Calendar = Calendar.getInstance()
    var day_of_week: Int = 1

    //传入的numSunday为-1,则代表最后一个周日
    if (-1 == numSunday.toInt) {
      cal.set(Calendar.YEAR, year)
      cal.set(Calendar.MONTH, month.toInt)
      cal.set(Calendar.DAY_OF_MONTH, 0)
      day_of_week = cal.get(Calendar.DAY_OF_WEEK)
      cal.add(Calendar.DAY_OF_MONTH, 1 - day_of_week)
    } else {
      cal.set(Calendar.YEAR, year)
      cal.set(Calendar.MONTH, month.toInt - 1)
      cal.set(Calendar.DAY_OF_MONTH, 1)
      day_of_week = cal.get(Calendar.DAY_OF_WEEK)
      // day_of_week 1 代表 星期日
      cal.add(Calendar.DAY_OF_MONTH, if (day_of_week == 1) (numSunday.toInt - 1) * 7 else numSunday.toInt * 7 - (day_of_week - 1))
    }

    if (month.toInt < 6) {
      if ("winner_time".equalsIgnoreCase(seasonType)) {
        //夏令时转冬令时,时间先设置为往后退一个小时,然后转成北京时间要加上时区的差值
        cal.set(Calendar.HOUR_OF_DAY, time.toInt - 1 + 8 - timezoneNumber.toInt)
      } else if ("summer_time".equalsIgnoreCase(seasonType)) {
        //冬令时转夏令时,相差为时区之差
        cal.set(Calendar.HOUR_OF_DAY, time.toInt + 8 - timezoneNumber.toInt)
      }
    } else {
      if ("winner_time".equalsIgnoreCase(seasonType)) {
        //冬令时转夏令时,相差为时区之差
        cal.set(Calendar.HOUR_OF_DAY, time.toInt + 8 - timezoneNumber.toInt)
      } else if ("summer_time".equalsIgnoreCase(seasonType)) {
        //夏令时转冬令时,时间先设置为往后退一个小时,然后转成北京时间要加上时区的差值
        cal.set(Calendar.HOUR_OF_DAY, time.toInt - 1 + 8 - timezoneNumber.toInt)
      }
    }
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)

    //println(day_of_week)
    val str: String = dateFormat.format(cal.getTime)
    //println(str)
    str
  }

  /**
    * 生成hbase的put语句
    */
  def getHbasePut() = {
    put = new Put(Bytes.toBytes(row_wid))

    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("warehouse_code"), Bytes.toBytes(warehouse_code))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("warehouse_name"), Bytes.toBytes(warehouse_name))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_year"), Bytes.toBytes(timezone_year))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_season_type"), Bytes.toBytes(timezone_season_type))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_season_start"), Bytes.toBytes(timezone_season_start))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_season_end"), Bytes.toBytes(timezone_season_end))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_summer_time_dif_val"), Bytes.toBytes(timezone_summer_time_dif_val))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_winner_time_dif_val"), Bytes.toBytes(timezone_winner_time_dif_val))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_summer_number"), Bytes.toBytes(timezone_summer_number))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_winner_number"), Bytes.toBytes(timezone_winner_number))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_number"), Bytes.toBytes(timezone_number))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("timezone_memo"), Bytes.toBytes(timezone_memo))

    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_insert_dt"), Bytes.toBytes(DateUtil.getNow()))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_update_dt"), Bytes.toBytes(DateUtil.getNow()))

    //添加到put集合中
    putList.add(put)
  }
}
