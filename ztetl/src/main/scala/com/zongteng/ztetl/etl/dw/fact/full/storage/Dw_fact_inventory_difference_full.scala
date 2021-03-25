package com.zongteng.ztetl.etl.dw.fact.full.storage

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_inventory_difference_full {
    //任务名称(一般同类名)
    private val task = "Dw_fact_inventory_difference_full"

    //dw层类名
    private val tableName = "fact_inventory_difference"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val gc_wms = "select id.row_wid " +
      " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
      " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
      " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
      " ,id.datasource_num_id as datasource_num_id" +
      " ,id.data_flag as  data_flag" +
      " ,id.integration_id as  integration_id" +
      " ,id.created_on_dt as created_on_dt" +
      " ,id.changed_on_dt as changed_on_dt" +
      " ,case " +
      " when tz.timezone_season_type='winner_time'  then  " +
      "     case when id.create_time between tz.timezone_season_start and tz.timezone_season_end then tz.timezone_winner_time_dif_val else tz.timezone_summer_time_dif_val end " +
      " when   tz.timezone_season_type='summer_time' then " +
      "     case when id.create_time between tz.timezone_season_start and tz.timezone_season_end then tz.timezone_summer_time_dif_val else tz.timezone_winner_time_dif_val end " +
      " else null end as timezone" +
      " ,null as exchange_rate" +
      " ,concat(id.datasource_num_id,id.warehouse_id) warehouse_key" +
      " ,wp.row_wid wp_key" +
      " ,id.di_id id_di_id" +
      " ,id.difference_code id_difference_code" +
      " ,id.warehouse_id id_warehouse_id" +
      " ,id.warehouse_code id_warehouse_code" +
      " ,wp.wp_id id_wp_id" +
      " ,id.wp_code id_wp_code" +
      " ,id.create_id id_create_id" +
      " ,id.update_id id_update_id" +
      " ,id.create_time id_create_time" +
      " ,id.update_time id_update_time" +
      " ,id.adjustment_code id_adjustment_code" +
      " ,id.difference_status id_difference_status" +
      " ,nvl(pvl1.vl_bi_name,id.difference_status) as id_difference_status_val" +
      " ,id.difference_count id_difference_count" +
      " ,date_format(id.created_on_dt,'yyyyMM') as month" +
      " from (select * from ods.gc_wms_inventory_difference where  data_flag<>'delete') as id" +
      s" left join ${Dw_dim_common.getDimSql("gc_wms_warehouse_physical","wp")} on wp.wp_code=id.wp_code" +
      " left join dw.par_timezone as tz on tz.warehouse_code=id.warehouse_code and  tz.timezone_year=year(id.create_time)" +
      " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='difference_status' and pvl.vl_datasource_table='gc_wms_inventory_difference') " +
      "            as pvl1 on pvl1.vl_value=id.difference_status"

    def main(args: Array[String]): Unit = {
      Dw_fact_common.getRunCode_hive_full_Into(task,tableName,Array(gc_wms))
    }
}
