package com.zongteng.ztetl.common

import java.util
import java.util.Calendar

import com.zongteng.ztetl.common.ParValListToHbase.insertToHbase
import com.zongteng.ztetl.util.DateUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * 插入节假日信息
  */
object ParHolidayToHbase {
  //private val warehouseCode: Array[String] = Array[String]("AU", "CNCQ", "CNGZ", "CNNB", "CNSH", "CNSZ", "CNTC", "CZ", "DE", "DESTROY", "ES", "FRVI", "HK", "IT", "JP", "RU", "TEST", "TESTONE", "TESTTOW", "UK", "UKFL", "UKOB", "USEA", "USNB", "USOT", "USSC", "USWE")
  private val warehouseCode: Array[String] = Array[String]("AU","CNCQ")
  private val yearNum: Array[Int] = Array[Int](2020,2021)
  //字段值
  private var warehouse_code: String = _
  private var holiday_date: String = _
  private var holiday_type: String = _
  private var holiday_type_val: String = _
  private var holiday_memo: String = _

  private var row_wid: String = _

  // 最终得到的put集合
  private val putList: util.ArrayList[Put] = new java.util.ArrayList[Put]
  // hbase列族
  private val column_family: String = "holiday"
  // dw层hbase表名
  private val table_name: String = "par_holiday_var"

  private var put:Put=null

  def main(args: Array[String]): Unit = {
    //循环得到所有要插入的仓库和年份的值
    warehouseCode.foreach((code: String) => {
      yearNum.foreach((yearNumber: Int) => {
        //给出仓库编号和年份,获取每天的日期和星期的值
        getDateInfo(code, yearNumber)
      })

    })

    //将数据写入hbase中
    val num: Int = insertToHbase(table_name, putList)

    println("一共插入了" + num + "条数据")
  }

  /**
    * 获取时间,并调用getHbasePut()方法,得到hbase的put语句
    * @param warehouseCode
    * @param yearNum
    */
  def getDateInfo(warehouseCode: String, yearNum: Int): Unit = {
    //时间
    val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd 00:00:00")
    val dateFormat2: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")
    warehouse_code = warehouseCode

    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.YEAR, yearNum)
    cal.set(Calendar.MONTH, 0)
    cal.set(Calendar.DATE, 1)
    do {
      row_wid = warehouse_code.concat("_").concat(dateFormat2.format(cal.getTime))
      holiday_date = dateFormat.format(cal.getTime)
      val i: Int = cal.get(Calendar.DAY_OF_WEEK)
      if (1 == i || 7 == i) {
        holiday_type = "1"
        holiday_type_val = "周六周日"
      } else {
        holiday_type = "0"
        holiday_type_val = "工作日"
      }
      holiday_memo = ""
      //添加到put中
      getHbasePut()
      println(row_wid + " " + warehouse_code + " " + holiday_date + " " + holiday_type + " " + holiday_type_val + " " + holiday_memo)

      cal.add(Calendar.DATE, 1)
      holiday_date = dateFormat.format(cal.getTime)
    } while (holiday_date.substring(0, 4).toInt == yearNum)
  }

  /**
    * 生成hbase的put语句
    */
  def getHbasePut() = {
    put = new Put(Bytes.toBytes(row_wid))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("warehouse_code"), Bytes.toBytes(warehouse_code))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("holiday_date"), Bytes.toBytes(holiday_date))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("holiday_type"), Bytes.toBytes(holiday_type))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("holiday_type_val"), Bytes.toBytes(holiday_type_val))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("holiday_memo"), Bytes.toBytes(holiday_memo))

    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_insert_dt"), Bytes.toBytes(DateUtil.getNow()))
    put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_update_dt"), Bytes.toBytes(DateUtil.getNow()))

    //添加到put集合中
    putList.add(put)
  }


}
