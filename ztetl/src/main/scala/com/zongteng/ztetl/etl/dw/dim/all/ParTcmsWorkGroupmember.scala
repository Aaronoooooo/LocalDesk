package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object ParTcmsWorkGroupmember {
  //任务名称(一般同类名)
  private val task = "ParTcmsWorkGroupmember"

  //dw层类名
  private val tableName = "par_tcms_work_groupmember"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_tcms="SELECT wgm.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,wgm.datasource_num_id as datasource_num_id" +
    " ,wgm.data_flag as data_flag" +
    " ,wgm.integration_id as integration_id" +
    " ,wgm.created_on_dt as created_on_dt" +
    " ,wgm.changed_on_dt as changed_on_dt" +
    " ,cast(concat(wgm.datasource_num_id,wgm.st_id) as bigint) as st_key" +
    " ,cast(concat(wgm.datasource_num_id,wgm.gp_id) as bigint) as wgp_key" +
    " ,wgm.gn_id as gp_gn_id" +
    " ,wgm.tms_id as gp_tms_id" +
    " ,wgm.gp_structurecode as gp_structurecode" +
    " ,wg.gp_shortname as gp_shortname" +
    " ,wgm.gp_worktype as gp_worktype" +
    " ,wgt.gp_worktype_name as gp_worktype_name" +
    " ,wgm.st_id as gp_st_id" +
    " ,st.st_name as gp_st_name" +
    " ,wgm.gm_shipperquota as gp_gm_shipperquota" +
    " ,wgm.gp_leader as gp_leader" +
    " ,wgm.gp_id as gp_id" +
    " ,wgm.last_update_time as gp_last_update_time" +
    " FROM (select * from ods.gc_tcms_wkg_groupmember where day= "+ nowDate +" ) as wgm" +
    " left join (select * from ods.gc_tcms_hmr_staff where day= "+ nowDate +") as st on st.st_id=wgm.st_id" +
    " left join (select * from ods.gc_tcms_wkg_grouptype where day= "+ nowDate +") as wgt on wgt.gp_worktype=wgm.gp_worktype" +
    " left join (select * from ods.gc_tcms_wkg_group where day= "+ nowDate +") as wg on wg.gp_id=wgm.gp_id"

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_tcms))
  }
}
