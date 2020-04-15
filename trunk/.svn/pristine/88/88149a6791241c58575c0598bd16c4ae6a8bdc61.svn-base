package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object DimTcmsCountry {

  private val task = "Spark_Etl_Dw_Dim_Tcms_Country"

  private val tableName = "dim_tcms_country"

  private val nowDate: String = DateUtil.getNowTime()

  val gc_tcms = "SELECT \n" +
    "   row_wid AS row_wid,\n" +
    "   date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "   current_timestamp ( ) AS w_insert_dt,\n" +
    "   current_timestamp ( ) AS w_update_dt,\n" +
    "   datasource_num_id AS datasource_num_id,\n" +
    "   data_flag AS data_flag,\n" +
    "   country_code AS integration_id,\n" +
    "   created_on_dt AS created_on_dt,\n" +
    "   changed_on_dt AS changed_on_dt,\n" +
    "\n" +
    "   country_code,\n" +
    "   country_cnname,\n" +
    "   country_enname,\n" +
    "   country_three_code,\n" +
    "   country_number,\n" +
    "   last_update_time AS country_last_update_time \n" +
    "FROM ods.gc_tcms_idd_country"

  def main(args: Array[String]): Unit = {
      Dw_dim_common.getRunCode_full(task, tableName, Array(gc_tcms))
  }

}
