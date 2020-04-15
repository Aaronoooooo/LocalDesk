package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.Dw_dim_common


object DimTcmsCity {

  private val task = "Spark_Etl_Dw_Dim_Tcms_City"

  private val tableName = "dim_tcms_city"

  private val gc_tcms = "SELECT\n" +
    "  row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  datasource_num_id AS datasource_num_id,\n" +
    "  data_flag AS data_flag,\n" +
    "  city_id AS integration_id,\n" +
    "  created_on_dt AS created_on_dt,\n" +
    "  changed_on_dt AS changed_on_dt,\n" +
    "\n" +
    "  city_id AS city_id,\n" +
    "  country_code AS city_country_code,\n" +
    "  state_code AS city_state_code,\n" +
    "  city_enname AS city_enname,\n" +
    "  city_cnname AS city_cnname,\n" +
    "  tms_id AS city_tms_id,\n" +
    "  last_update_time AS city_last_update_time\n" +
    s"FROM " + Dw_dim_common.getDimSql("gc_tcms_idd_city", "alias_idd_city")

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_tcms))
  }

}
