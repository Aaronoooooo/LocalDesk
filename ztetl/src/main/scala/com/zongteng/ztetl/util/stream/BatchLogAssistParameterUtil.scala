package com.zongteng.ztetl.util.stream

import com.zongteng.ztetl.common.SystemCodeUtil
import com.zongteng.ztetl.entity.common.AssistParameter
import org.apache.commons.lang.StringUtils

object BatchLogAssistParameterUtil {
  /**
    * @param database 数据库
    * @param table    表名
    */
  def getEntityObject(database: String, table: String) = {

    assert(StringUtils.isNotBlank(database), "数据库不能为空")
    assert(StringUtils.isNotBlank(table), "表名不能为空")

    val dt: String = database + "." + table.substring(0,19)

    dt match {
      //日志表
      case "gc_inventory_batch_log.inventory_batch_log" => gc_inventory_batch_log$inventory_batch_log(database, table)
      case _ => throw new Exception("数据库 {" + database + "} 中表{" + table + "} 对应的方法不存在,请添加")
    }
  }

  def gc_inventory_batch_log$inventory_batch_log(database: String, table: String): AssistParameter = {
    val clazz: Class[_] = classOf[com.zongteng.ztetl.entity.gc_wms_ibl.InventoryBatchLog]
    val primaryKey: String = "ibl_id"
    val cf: String = "inventory_batch_log"
    val datasource_num_id: String = SystemCodeUtil.GC_WMS_IBL
    val add_time_field = "ibl_add_time"
    val update_time_field_1 = ""
    val update_time_field_2: String = ""
    check(AssistParameter(clazz, primaryKey, cf, datasource_num_id, add_time_field, update_time_field_1, update_time_field_2, database, table))
  }


  def check(assistParameter: AssistParameter) = {
    assert(assistParameter.className != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的样例类不存在,请添加")
    assert(assistParameter.primaryKey != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的主键不存在,请添加")
    assert(assistParameter.cf != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的列族不存在,请添加")
    assert(assistParameter.datasource_num_id != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的数据来源不存在,请添加")
    assert(assistParameter.add_time_field != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的新增时间不存在,请添加")
    assert(assistParameter.update_time_field_1 != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的修改时间1_时间戳不存在,请添加")
    assert(assistParameter.update_time_field_2 != null, "数据库 {" + assistParameter.database + "} 中表{" + assistParameter.table + "} 对应的修改时间2_修改时间不存在,请添加")
    assistParameter
  }
}
