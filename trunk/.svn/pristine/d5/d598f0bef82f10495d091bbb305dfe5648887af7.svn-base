package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.Dw_fact_common

/**
  * 波次分区表
  */
object Dw_fact_wellen_area_full {
  //任务名称(一般同类名)
  private val task = "Dw_fact_wellen_area_full"

  //dw层类名
  private val tableName = "fact_wellen_area"

  private val odsTables = Array(
    "gc_owms_au_wellen_area",
    "gc_owms_cz_wellen_area",
    "gc_owms_de_wellen_area",
    "gc_owms_es_wellen_area",
    "gc_owms_it_wellen_area",
    "gc_owms_jp_wellen_area",
    "gc_owms_uk_wellen_area",
    "gc_owms_frvi_wellen_area",
    "gc_owms_ukob_wellen_area",
    "gc_owms_usnb_wellen_area",
    "gc_owms_usot_wellen_area",
    "gc_owms_ussc_wellen_area",
    "gc_owms_usea_wellen_area",
    "gc_owms_uswe_wellen_area",
    "zy_owms_au_wellen_area",
    "zy_owms_cz_wellen_area",
    "zy_owms_de_wellen_area",
    "zy_owms_ru_wellen_area",
    "zy_owms_uk_wellen_area",
    "zy_owms_ussc_wellen_area",
    "zy_owms_usea_wellen_area",
    "zy_owms_uswe_wellen_area"
  )

  private val sql = "SELECT wa.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,wa.datasource_num_id as datasource_num_id" +
    " ,wa.data_flag as data_flag" +
    " ,wa.integration_id as integration_id" +
    " ,wa.created_on_dt as created_on_dt" +
    " ,wa.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,wa.wellen_area_id as wellen_area_id" +
    " ,wa.wellen_id as wellen_id" +
    " ,wa.wa_code as wellen_wa_code" +
    " FROM (select * from ods.wellenArea where data_flag<>'delete') as wa"

  def makeSelectSql() = {
    odsTables.map(sql.replace("wellenArea", _))
  }

  def main(args: Array[String]): Unit = {
    val selectSql: Array[String] = makeSelectSql()
    Dw_fact_common.getRunCode_hive_nopartition_full_Into(task, tableName, selectSql)
  }

}
