package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object DimTcmsWorkGroup {
  //任务名称(一般同类名)
  private val task = "DimTcmsWorkGroup"

  //dw层类名
  private val tableName = "dim_tcms_work_group"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_tcms="SELECT wg.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,wg.datasource_num_id as datasource_num_id" +
    " ,wg.data_flag as data_flag" +
    " ,wg.integration_id as integration_id" +
    " ,wg.created_on_dt as created_on_dt" +
    " ,wg.changed_on_dt as changed_on_dt" +
    " ,wg.gp_id as wgp_id" +
    " ,wg.tms_id as wgp_tms_id" +
    " ,wg.gp_structurecode as wgp_structurecode" +
    " ,wg.gp_shortname as wgp_shortname" +
    " ,wg.gp_worktype as wgp_worktype" +
    " ,wgt.gp_worktype_name as wgp_worktype_name" +
    " ,wgt.gp_worktype_ename as wgp_worktype_ename" +
    " ,wgt.gp_worktype_note as wgp_worktype_note" +
    " ,wg.last_update_time as wgp_last_update_time" +
    " FROM (select * from ods.gc_tcms_wkg_group where day="+ nowDate +" ) as wg " +
    " left join (select * from ods.gc_tcms_wkg_grouptype where day="+ nowDate +" ) as wgt on wgt.gp_worktype=wg.gp_worktype"

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_tcms))
  }
}
