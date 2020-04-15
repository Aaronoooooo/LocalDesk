package com.zongteng.ztetl.etl.dw.fact.full.orders

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_order_log_full {
  //任务名称(一般同类名)
  private val task = "Dw_fact_order_log_full"

  //dw层类名
  private val tableName = "fact_order_log"

  // 获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms ="SELECT ol.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ol.datasource_num_id as datasource_num_id" +
    " ,ol.data_flag as data_flag" +
    " ,ol.integration_id as integration_id" +
    " ,ol.created_on_dt as created_on_dt" +
    " ,ol.changed_on_dt as changed_on_dt" +
    " ,case when (ptz.timezone_season_type='summer_time' and ol.ol_add_time between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and ol.ol_add_time not between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_summer_number" +
    " when  (ptz.timezone_season_type='summer_time' and ol.ol_add_time not between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and ol.ol_add_time  between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_winner_number" +
    " else null end " +
    " as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(ol.datasource_num_id,od.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(ol.datasource_num_id,od.to_warehouse_id) as bigint) as to_warehouse_key" +
    " ,cast(concat(ol.datasource_num_id,od.customer_id) as bigint) as customer_key" +
    " ,cast(concat(ol.datasource_num_id,od.sc_id) as bigint) as sc_key" +
    " ,ol.ol_id as ol_id" +
    " ,ol.order_id as ol_order_id" +
    " ,ol.order_code as ol_order_code" +
    " ,ol.ol_type as ol_type" +
    " ,ol.order_status_from as ol_order_status_from" +
    " ,ol.order_status_to as ol_order_status_to" +
    " ,ol.user_id as ol_user_id" +
    " ,ol.ol_ip as ol_ip" +
    " ,ol.ol_comments as ol_comments" +
    " ,ol.ol_add_time as ol_add_time" +
    " ,wa.warehouse_code as ol_warehouse_code" +
    " ,from_unixtime(unix_timestamp(ol.created_on_dt),'yyyyMM') as month" +
    " FROM (select * from ods.gc_wms_order_log where data_flag<>'delete' ) as ol " +
    " left join (select * from ods.gc_wms_orders where data_flag<>'delete' ) as od on od.order_id=ol.order_id" +
    s" left join ${Dw_dim_common.getDimSql("gc_wms_warehouse","wa")} on wa.warehouse_id=od.warehouse_id" +
    " left join dw.par_timezone as ptz on ptz.warehouse_code=wa.warehouse_code and date_format(ol.ol_add_time,'yyyy')=ptz.timezone_year"
  private val zy_wms ="SELECT ol.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ol.datasource_num_id as datasource_num_id" +
    " ,ol.data_flag as data_flag" +
    " ,ol.integration_id as integration_id" +
    " ,ol.created_on_dt as created_on_dt" +
    " ,ol.changed_on_dt as changed_on_dt" +
    " ,case when (ptz.timezone_season_type='summer_time' and ol.ol_add_time between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and ol.ol_add_time not between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_summer_number" +
    " when  (ptz.timezone_season_type='summer_time' and ol.ol_add_time not between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and ol.ol_add_time  between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_winner_number" +
    " else null end " +
    " as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(ol.datasource_num_id,od.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(ol.datasource_num_id,od.to_warehouse_id) as bigint) as to_warehouse_key" +
    " ,cast(concat(ol.datasource_num_id,od.customer_id) as bigint) as customer_key" +
    " ,cast(concat(ol.datasource_num_id,od.sc_id) as bigint) as sc_key" +
    " ,ol.ol_id as ol_id" +
    " ,ol.order_id as ol_order_id" +
    " ,ol.order_code as ol_order_code" +
    " ,ol.ol_type as ol_type" +
    " ,ol.order_status_from as ol_order_status_from" +
    " ,ol.order_status_to as ol_order_status_to" +
    " ,ol.user_id as ol_user_id" +
    " ,ol.ol_ip as ol_ip" +
    " ,ol.ol_comments as ol_comments" +
    " ,ol.ol_add_time as ol_add_time" +
    " ,wa.warehouse_code as ol_warehouse_code" +
    " ,from_unixtime(unix_timestamp(ol.created_on_dt),'yyyyMM') as month" +
    " FROM (select * from ods.zy_wms_order_log where data_flag<>'delete' ) as ol" +
    " left join (select * from ods.zy_wms_orders where data_flag<>'delete' ) as od on od.order_id=ol.order_id" +
    s" left join ${Dw_dim_common.getDimSql("zy_wms_warehouse","wa")} on wa.warehouse_id=od.warehouse_id" +
    " left join dw.par_timezone as ptz on ptz.warehouse_code=wa.warehouse_code and date_format(ol.ol_add_time,'yyyy')=ptz.timezone_year"

  def main(args: Array[String]): Unit = {
    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, Array(gc_wms,zy_wms),Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }
}
