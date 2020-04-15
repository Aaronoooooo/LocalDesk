package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object DimEmployeePosition {
  //任务名称(一般同类名)
  private val task = "Dw_dim_employee_position"

  //dw层类名
  private val tableName = "dim_employee_position"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms = "select" +
    " p.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " p.datasource_num_id AS datasource_num_id," +
    " p.data_flag AS data_flag," +
    " p.integration_id AS integration_id," +
    " p.created_on_dt AS created_on_dt," +
    " p.changed_on_dt AS changed_on_dt," +
    " cast(CONCAT(p.datasource_num_id, p.ud_id) as bigint) as ed_key," +
    " cast(CONCAT(p.datasource_num_id, p.upl_id) as bigint) as epl_key," +
    " p.up_id as ep_id," +
    " p.up_name as ep_name," +
    " p.up_name_en as ep_name_en," +
    " p.ud_id as ep_ud_id," +
    " pl.upl_id as ep_upl_id," +
    " p.remarks as ep_remarks," +
    " pl.upl_name as ep_upl_name," +
    " pl.upl_name_en as ep_upl_name_en" +
    " from (SELECT * from ods.gc_wms_user_position  where day=" + nowDate + ") as p" +
    " left join (SELECT * from ods.gc_wms_user_position_level  where day=" + nowDate + ") as pl on p.upl_id = pl.upl_id"
  private val zy_wms = "  select " +
    " p.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " p.datasource_num_id AS datasource_num_id," +
    " p.data_flag AS data_flag," +
    " p.integration_id AS integration_id," +
    " p.created_on_dt AS created_on_dt," +
    " p.changed_on_dt AS changed_on_dt," +
    " cast(CONCAT(p.datasource_num_id, p.ud_id) as bigint) as ed_key," +
    " cast(CONCAT(p.datasource_num_id, p.upl_id) as bigint) as epl_key," +
    " p.up_id as ep_id," +
    " p.up_name as ep_name," +
    " p.up_name_en as ep_name_en," +
    " p.ud_id as ep_ud_id," +
    " p.upl_id as ep_upl_id," +
    " null as ep_remarks," +
    " pl.upl_name as ep_upl_name," +
    " pl.upl_name_en as ep_upl_name_en" +
    " from (SELECT * from ods.zy_wms_user_position where day=" + nowDate + ") as  p" +
    " left join (SELECT * from ods.zy_wms_user_position_level where day=" + nowDate + ") as pl  on p.upl_id = pl.upl_id"

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_wms, zy_wms))
  }
}
