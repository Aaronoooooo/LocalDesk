package com.zongteng.ztetl.etl.dw.fact.full.storage

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_take_stock_assignment_full {
  //任务名称(一般同类名)
  private val task = "Dw_fact_take_stock_assignment_full"

  //dw层类名
  private val tableName = "fact_take_stock_assignment"

  // 获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms = "select tsa.row_wid " +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,tsa.datasource_num_id as datasource_num_id" +
    " ,tsa.data_flag as  data_flag" +
    " ,tsa.integration_id as  integration_id" +
    " ,tsa.created_on_dt as created_on_dt" +
    " ,tsa.changed_on_dt as changed_on_dt" +
    " ,case " +
    " when tz.timezone_season_type='winner_time'  then  " +
    "     case when tsa.tsa_add_time between tz.timezone_season_start and tz.timezone_season_end then tz.timezone_winner_time_dif_val else tz.timezone_summer_time_dif_val end " +
    " when   tz.timezone_season_type='summer_time' then " +
    "     case when tsa.tsa_add_time between tz.timezone_season_start and tz.timezone_season_end then tz.timezone_summer_time_dif_val else tz.timezone_winner_time_dif_val end " +
    " else null end as timezone" +
    " ,null as exchange_rate" +
    " ,concat(w.datasource_num_id,w.warehouse_id) warehouse_key" +
    " ,concat(tsa.datasource_num_id,tsi.product_id) product_key" +
    " ,c.row_wid customer_key" +
    " ,lc.row_wid lc_key" +
    " ,wp.row_wid wp_key" +
    " ,tsa.tsa_id" +
    " ,tsa.tsi_id tsa_tsi_id" +
    " ,tsa.ts_code tsa_ts_code" +
    " ,tsa.user_id tsa_user_id" +
    " ,tsa.tsa_quantity" +
    " ,tsa.tsa_type" +
    " ,nvl(pvl1.vl_bi_name,tsa.tsa_type) as tsa_type_val" +
    " ,tsa.tsa_status" +
    " ,nvl(pvl2.vl_bi_name,tsa.tsa_status) as tsa_status_val" +
    " ,tsa.tsa_add_time" +
    " ,ts.warehouse_id tsa_warehouse_id" +
    " ,w.warehouse_code tsa_warehouse_code" +
    " ,wp.wp_id tsa_wp_id" +
    " ,wp.wp_code tsa_wp_code " +
    " ,tsi.product_id tsa_product_id" +
    " ,tsi.product_barcode tsa_product_barcode" +
    " ,c.customer_id tsa_customer_id" +
    " ,tsi.customer_code tsa_customer_code" +
    " ,lc.lc_id tsa_lc_id" +
    " ,tsi.lc_code tsa_lc_code" +
    " ,date_format(tsa.created_on_dt,'yyyyMM') as month" +
    " from  (select * from ods.gc_wms_take_stock_assignment where data_flag<>'delete') as tsa" +
    " left join (select * from ods.gc_wms_take_stock_item where data_flag<>'delete') as tsi on tsi.tsi_id=tsa.tsi_id" +
    " left join  (select * from ods.gc_wms_take_stock where data_flag<>'delete') as ts on tsa.ts_code=ts.ts_code" +
    s" left join  ${Dw_dim_common.getDimSql("gc_wms_warehouse","w")} on w.warehouse_id=ts.warehouse_id" +
    s" left join ${Dw_dim_common.getDimSql("gc_wms_warehouse_physical","wp")} on wp.wp_code=ts.wp_code" +
    s" left join  ${Dw_dim_common.getDimSql("gc_wms_location","lc")} on lc.warehouse_id=ts.warehouse_id and  lc.lc_code=tsi.lc_code" +
    s" left join ${Dw_dim_common.getDimSql("gc_wms_customer","c")} on c.customer_code=tsi.customer_code" +
    " left join dw.par_timezone as tz on tz.warehouse_code=w.warehouse_code and  tz.timezone_year=year(tsa.tsa_add_time)" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='tsa_type' and pvl.vl_datasource_table='gc_wms_take_stock_assignment') " +
    "            as pvl1 on pvl1.vl_value=tsa.tsa_type" +
    " left join  (select * from dw.par_val_list as pv1 where pv1.datasource_num_id='9004' and pv1.vl_type='tsa_status' and pv1.vl_datasource_table='gc_wms_take_stock_assignment') " +
    "            as pvl2 on pvl2.vl_value=tsa.tsa_status"
  private val zy_wms = "select tsa.row_wid " +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,tsa.datasource_num_id as datasource_num_id" +
    " ,tsa.data_flag as  data_flag" +
    " ,tsa.integration_id as  integration_id" +
    " ,tsa.created_on_dt as created_on_dt" +
    " ,tsa.changed_on_dt as changed_on_dt" +
    " ,case " +
    " when tz.timezone_season_type='winner_time'  then  " +
    "     case when tsa.tsa_add_time between tz.timezone_season_start and tz.timezone_season_end then tz.timezone_winner_time_dif_val else tz.timezone_summer_time_dif_val end " +
    " when   tz.timezone_season_type='summer_time' then " +
    "     case when tsa.tsa_add_time between tz.timezone_season_start and tz.timezone_season_end then tz.timezone_summer_time_dif_val else tz.timezone_winner_time_dif_val end " +
    " else null end as timezone" +
    " ,null as exchange_rate" +
    " ,concat(w.datasource_num_id,w.warehouse_id) warehouse_key" +
    " ,concat(tsa.datasource_num_id,tsi.product_id) product_key" +
    " ,c.row_wid customer_key" +
    " ,lc.row_wid lc_key" +
    " ,null wp_key" +
    " ,tsa.tsa_id" +
    " ,tsa.tsi_id tsa_tsi_id" +
    " ,tsa.ts_code tsa_ts_code" +
    " ,tsa.user_id tsa_user_id" +
    " ,tsa.tsa_quantity" +
    " ,tsa.tsa_type" +
    " ,nvl(pvl1.vl_bi_name,tsa.tsa_type) as tsa_type_val" +
    " ,tsa.tsa_status" +
    " ,nvl(pvl2.vl_bi_name,tsa.tsa_status) as tsa_status_val" +
    " ,tsa.tsa_add_time" +
    " ,ts.warehouse_id tsa_warehouse_id" +
    " ,w.warehouse_code tsa_warehouse_code" +
    " ,null tsa_wp_id" +
    " ,null tsa_wp_code " +
    " ,tsi.product_id tsa_product_id" +
    " ,tsi.product_barcode tsa_product_barcode" +
    " ,c.customer_id tsa_customer_id" +
    " ,tsi.customer_code tsa_customer_code" +
    " ,lc.lc_id tsa_lc_id" +
    " ,tsi.lc_code tsa_lc_code" +
    " ,date_format(tsa.created_on_dt,'yyyyMM') as month" +
    " from  (select * from ods.zy_wms_take_stock_assignment where data_flag<>'delete') as tsa" +
    " left join (select * from ods.zy_wms_take_stock_item where data_flag<>'delete') as tsi on tsi.tsi_id=tsa.tsi_id" +
    " left join (select * from ods.zy_wms_take_stock where data_flag<>'delete') as ts on tsa.ts_code=ts.ts_code" +
    s" left join  ${Dw_dim_common.getDimSql("zy_wms_warehouse","w")} on w.warehouse_id=ts.warehouse_id" +
    s" left join  ${Dw_dim_common.getDimSql("zy_wms_location","lc")} on lc.warehouse_id=ts.warehouse_id and  lc.lc_code=tsi.lc_code" +
    s" left join ${Dw_dim_common.getDimSql("zy_wms_customer","c")} on c.customer_code=tsi.customer_code" +
    " left join dw.par_timezone as tz on tz.warehouse_code=w.warehouse_code and  tz.timezone_year=year(tsa.tsa_add_time)" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='tsa_type' and pvl.vl_datasource_table='zy_wms_take_stock_assignment') " +
    "            as pvl1 on pvl1.vl_value=tsa.tsa_type" +
    " left join  (select * from dw.par_val_list as pv1 where pv1.datasource_num_id='9022' and pv1.vl_type='tsa_status' and pv1.vl_datasource_table='zy_wms_take_stock_assignment') " +
    "            as pvl2 on pvl2.vl_value=tsa.tsa_status"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms,zy_wms).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_fact_common.getRunCode_hive_full_Into(task,tableName,sqlArray,Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))
  }
}
