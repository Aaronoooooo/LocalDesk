package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.Dw_fact_common

object DwFactWellenScFull {

  // 任务名称
  val task = "Spark_Etl_Dw_Fact_Wellen_Sc"

  // dw层表名
  val tableName = "fact_wellen_sc"

  val selectSql = "SELECT \n" +
    "  wsc.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  wsc.datasource_num_id AS datasource_num_id,\n" +
    "  wsc.data_flag AS data_flag,\n" +
    "  wsc.wellen_sc_id AS integration_id,\n" +
    "  wsc.created_on_dt AS created_on_dt,\n" +
    "  wsc.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "\n" +
    "  concat(wsc.datasource_num_id, wsc.wellen_id) AS wellen_key,\n" +
    "\n" +
    "  wsc.wellen_sc_id AS wellen_sc_id,\n" +
    "  wsc.sc_code AS wellen_sc_code,\n" +
    "  wsc.wellen_id AS wellen_id\n" +
    "FROM (SELECT * FROM ods.wellenScTable WHERE data_flag != 'DELETE') wsc"

  val odsTalbes = Array(
    "gc_owms_au_wellen_sc",
    "gc_owms_cz_wellen_sc",
    "gc_owms_de_wellen_sc",
    "gc_owms_es_wellen_sc",
    "gc_owms_frvi_wellen_sc",
    "gc_owms_it_wellen_sc",
    "gc_owms_jp_wellen_sc",
    "gc_owms_uk_wellen_sc",
    "gc_owms_ukob_wellen_sc",
    "gc_owms_usea_wellen_sc",
    "gc_owms_uswe_wellen_sc",
    "gc_owms_usnb_wellen_sc",
    "gc_owms_usot_wellen_sc",
    "gc_owms_ussc_wellen_sc",
    "zy_owms_au_wellen_sc",
    "zy_owms_cz_wellen_sc",
    "zy_owms_de_wellen_sc",
    "zy_owms_ru_wellen_sc",
    "zy_owms_uk_wellen_sc",
    "zy_owms_usea_wellen_sc",
    "zy_owms_uswe_wellen_sc",
    "zy_owms_ussc_wellen_sc"
  )

  def makeSelectSql() = {
    odsTalbes.map(selectSql.replace("wellenScTable", _))
  }

  def main(args: Array[String]): Unit = {

    val selectSqls: Array[String] = makeSelectSql

    Dw_fact_common.getRunCode_hive_nopartition_full_Into(task, tableName, selectSqls)
  }

}
