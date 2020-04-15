package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimZone {
  //任务名称(一般同类名)
  private val task = "DimZone"

  //dw层类名
  private val tableName = "dim_zone"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()
  //要执行的sql语句
  private val gc_wms ="SELECT zc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,zc.datasource_num_id as datasource_num_id" +
    " ,zc.data_flag as data_flag" +
    " ,zc.integration_id as integration_id" +
    " ,zc.created_on_dt as created_on_dt" +
    " ,zc.changed_on_dt as changed_on_dt" +
    " ,cast(concat(zc.datasource_num_id,zc.zs_id) as bigint) as zs_key" +
    " ,zc.zc_id as zone_id" +
    " ,zc.zs_id as zone_zs_id" +
    " ,zc.zc_name as zone_name" +
    " ,zc.zc_name_en as zone_name_en" +
    " ,zc.zc_code as zone_code" +
    " ,zc.zc_sort as zone_sort" +
    " ,zc.zc_note as zone_note" +
    " ,zc.zc_update_time as zone_update_time" +
    " FROM (select * from ods.gc_wms_sp_zone_charge where day="+ nowDate +" ) as zc"
  private val zy_wms ="SELECT zc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,zc.datasource_num_id as datasource_num_id" +
    " ,zc.data_flag as data_flag" +
    " ,zc.integration_id as integration_id" +
    " ,zc.created_on_dt as created_on_dt" +
    " ,zc.changed_on_dt as changed_on_dt" +
    " ,cast(concat(zc.datasource_num_id,zc.zs_id) as bigint) as zs_key" +
    " ,zc.zc_id as zone_id" +
    " ,zc.zs_id as zone_zs_id" +
    " ,zc.zc_name as zone_name" +
    " ,zc.zc_name_en as zone_name_en" +
    " ,zc.zc_code as zone_code" +
    " ,zc.zc_sort as zone_sort" +
    " ,zc.zc_note as zone_note" +
    " ,zc.zc_update_time as zone_update_time" +
    " FROM (select * from ods.zy_wms_sp_zone_charge where day="+ nowDate +" ) as zc"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms,zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task,tableName,sqlArray,Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))


  }
}
