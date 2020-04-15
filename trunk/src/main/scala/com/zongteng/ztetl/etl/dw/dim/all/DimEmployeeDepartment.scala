package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache}
import com.zongteng.ztetl.util.DateUtil

object DimEmployeeDepartment {

  //任务名称(一般同类名)
  private val task = "Dw_dim_employee_department"

  //dw层类名
  private val tableName = "dim_employee_department"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms="SELECT" +
    " ud.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " ud.datasource_num_id AS datasource_num_id," +
    " ud.data_flag AS data_flag," +
    " ud.integration_id AS integration_id," +
    " ud.created_on_dt AS created_on_dt," +
    " ud.changed_on_dt AS changed_on_dt," +
    " ud.ud_id as ed_id," +
    " ud.ud_name as ed_name," +
    " ud.ud_name_en as ed_name_en," +
    " ud.ud_sort as ed_sort," +
    " ud.ud_supervisor_id as ed_supervisor_id" +
    " FROM" +
    "  (select * from ods.gc_wms_user_department where day="+ nowDate +") as ud"
  private val zy_wms="SELECT" +
    " ud.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " ud.datasource_num_id AS datasource_num_id," +
    " ud.data_flag AS data_flag," +
    " ud.integration_id AS integration_id," +
    " ud.created_on_dt AS created_on_dt," +
    " ud.changed_on_dt AS changed_on_dt," +
    " ud.ud_id as ed_id ," +
    " ud.ud_name as ed_name," +
    " ud.ud_name_en as ed_name_en," +
    " ud.ud_sort as ed_sort," +
    " ud.ud_supervisor_id as ed_supervisor_id" +
    " FROM" +
    " (select * from ods.zy_wms_user_department where day="+ nowDate +") as ud"

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_wms,zy_wms),Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }
}
