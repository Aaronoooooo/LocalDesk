package com.zongteng.ztetl.etl.stream

import java.{io, util}

import com.google.gson.Gson
import com.zongteng.ztetl.entity.common.InsertModel
import com.zongteng.ztetl.util.DataNullUtil
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ListBuffer
object Stream_json_util {

  private val gson: Gson = new Gson()

  /**
    * @param entityClass 样例类的类对象
    * @param insertModel 数据信息
    * @return
    */
  def getCaseClass(insertModel: InsertModel, entityClass: Class[_])    = {

    val buffer: ListBuffer[io.Serializable] = new ListBuffer[io.Serializable]()

    // 获取数据,可能里面有多条
    val maps: Array[util.HashMap[String, String]] = insertModel.data

    // 样例类的Class
   // val entityClass: Class[_] = Class.forName(entityClassName)

    // 如果data为null，那么返回一个空集合
    if (maps != null && maps.size > 0) {

      for (map <- maps) {
        // 转换成样例类
        val entityObject: io.Serializable = gson.fromJson(gson.toJson(map), entityClass)
        buffer += entityObject
      }
    }

    buffer.foreach(println)
    buffer
  }


  /**
    * @param entityClassName 全类名
    * @param insertModel 数据信息
    * @return
    */
  def getCaseClass(insertModel: InsertModel, entityClassName: String)    = {

    val buffer: ListBuffer[io.Serializable] = new ListBuffer[io.Serializable]()

    // 获取数据,可能里面有多条
    val maps: Array[util.HashMap[String, String]] = insertModel.data

    // 样例类的Class
    val entityClass: Class[_] = Class.forName(entityClassName)

    // 如果data为null，那么返回一个空集合
    if (maps != null && maps.size > 0) {

      for (map <- maps) {
        // 转换成样例类
        val entityObject: io.Serializable = gson.fromJson(gson.toJson(map), entityClass)
        buffer += entityObject
      }
    }

    buffer
  }


  def getHBaseTable(jsonStr: String) = {
    getInsertObject(jsonStr).table
  }

  def getInsertObject(jsonStr: String) = {

    var insertModel: InsertModel = null

    if (StringUtils.isNotBlank(jsonStr)) {
      insertModel = gson.fromJson(jsonStr, classOf[InsertModel])
    }

    insertModel
  }

  /**
    * 获取源系统修改时间
    * @param update_time_1
    * @param update_time_2
    * @return
    */
  def getChangedOnDt(update_time_1: String, update_time_2: String) = {

    val changed_on_dt: String = if (!DataNullUtil.dtIsNull(update_time_1) && !DataNullUtil.dtIsNull(update_time_2)) {
      update_time_1
    } else if (!DataNullUtil.dtIsNull(update_time_1)) {
      update_time_1
    } else if (!DataNullUtil.dtIsNull(update_time_2)) {
      update_time_2
    } else {
      null
    }

    DataNullUtil.nvlDateTime(changed_on_dt)
  }

}

