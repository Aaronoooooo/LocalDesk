package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object DimTcmsEmployee {

  private val task = "Spark_Etl_Dim_Tcms_Employee"

  private val tableName = "dim_tcms_employee"

  private val nowDate: String = DateUtil.getNowTime()

  private val gc_tcms = "SELECT \n" +
    "  sta.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  sta.datasource_num_id AS datasource_num_id,\n" +
    "  sta.data_flag AS data_flag,\n" +
    "  sta.st_id AS integration_id,\n" +
    "  sta.created_on_dt AS created_on_dt,\n" +
    "  sta.changed_on_dt AS changed_on_dt,\n" +
    "\n" +
    "  st_id AS employee_st_id,\n" +
    "  st_code AS employee_st_code,\n" +
    "  vs_code AS employee_vs_code,\n" +
    "  og_id AS employee_og_id,\n" +
    "  competence_og_id AS employee_competence_og_id,\n" +
    "  st_name AS employee_st_name,\n" +
    "  st_ename AS employee_st_ename,\n" +
    "  ur_loginpassword AS employee_ur_loginpassword,\n" +
    "  sp_id AS employee_sp_id,\n" +
    "  st_id_ctreate AS employee_st_id_ctreate,\n" +
    "  tms_id AS employee_tms_id,\n" +
    "  error_count AS employee_error_count,\n" +
    "  last_obtain_time AS employee_last_obtain_time,\n" +
    "  last_update_time AS employee_last_update_time\n" +
    s"FROM (SELECT * FROM ods.gc_tcms_hmr_staff WHERE day = '$nowDate') AS sta "

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_tcms))
  }


}
