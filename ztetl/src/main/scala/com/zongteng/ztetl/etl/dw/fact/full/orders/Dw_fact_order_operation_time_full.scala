package com.zongteng.ztetl.etl.dw.fact.full.orders

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache}

object Dw_fact_order_operation_time_full {
  //任务名称(一般同类名)
  private val task = "Dw_fact_order_operation_time_full"

  //dw层类名
  private val tableName = "fact_order_operation_time"

  //要执行的sql语句
  private val gc_wms ="SELECT oot.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,oot.datasource_num_id as datasource_num_id" +
    " ,oot.data_flag as data_flag" +
    " ,oot.integration_id as integration_id" +
    " ,oot.created_on_dt as created_on_dt" +
    " ,oot.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(oot.datasource_num_id,od.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(oot.datasource_num_id,od.to_warehouse_id) as bigint) as to_warehouse_key" +
    " ,cast(concat(oot.datasource_num_id,od.customer_id) as bigint) as customer_key" +
    " ,cast(concat(oot.datasource_num_id,od.sc_id) as bigint) as sc_key" +
    " ,oot.oot_id as oot_id" +
    " ,oot.order_id as oot_order_id" +
    " ,oot.order_code as oot_order_code" +
    " ,oot.cutoff_finish_time as oot_cutoff_finish_time" +
    " ,oot.cutoff_time as oot_cutoff_time" +
    " ,oot.submit_time as oot_submit_time" +
    " ,oot.abnormal_time as oot_abnormal_time" +
    " ,oot.process_time as oot_process_time" +
    " ,oot.pack_time as oot_pack_time" +
    " ,oot.ship_time as oot_ship_time" +
    " ,oot.import_time as oot_import_time" +
    " ,oot.delivered_time as oot_delivered_time" +
    " ,oot.sync_time as oot_sync_time" +
    " ,oot.update_time as oot_update_time" +
    " ,oot.abnormal_user_id as oot_abnormal_user_id" +
    " ,oot.process_user_id as oot_process_user_id" +
    " ,oot.pack_user_id as oot_pack_user_id" +
    " ,oot.import_user_id as oot_import_user_id" +
    " ,oot.ship_user_id as oot_ship_user_id" +
    " ,oot.delivered_user_id as oot_delivered_user_id" +
    " ,oot.cutoff_user_id as oot_cutoff_user_id" +
    " ,oot.cutoff_finish_user_id as oot_cutoff_finish_user_id" +
    " ,oot.oot_timestamp as oot_timestamp" +
    " ,oot.abnormal_deal_time as oot_abnormal_deal_time" +
    " ,oot.abnormal_deal_user_id as oot_abnormal_deal_user_id" +
    " ,oot.ascan_time as oot_ascan_time" +
    " FROM (select * from ods.gc_wms_order_operation_time where data_flag<>'delete' ) as oot" +
    " left join (select * from ods.gc_wms_orders where data_flag<>'delete' ) as od on od.order_id=oot.order_id"
  private val zy_wms ="SELECT oot.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,oot.datasource_num_id as datasource_num_id" +
    " ,oot.data_flag as data_flag" +
    " ,oot.integration_id as integration_id" +
    " ,oot.created_on_dt as created_on_dt" +
    " ,oot.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(oot.datasource_num_id,od.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(oot.datasource_num_id,od.to_warehouse_id) as bigint) as to_warehouse_key" +
    " ,cast(concat(oot.datasource_num_id,od.customer_id) as bigint) as customer_key" +
    " ,cast(concat(oot.datasource_num_id,od.sc_id) as bigint) as sc_key" +
    " ,oot.oot_id as oot_id" +
    " ,oot.order_id as oot_order_id" +
    " ,oot.order_code as oot_order_code" +
    " ,oot.cutoff_finish_time as oot_cutoff_finish_time" +
    " ,oot.cutoff_time as oot_cutoff_time" +
    " ,oot.submit_time as oot_submit_time" +
    " ,oot.abnormal_time as oot_abnormal_time" +
    " ,oot.process_time as oot_process_time" +
    " ,oot.pack_time as oot_pack_time" +
    " ,oot.ship_time as oot_ship_time" +
    " ,oot.import_time as oot_import_time" +
    " ,oot.delivered_time as oot_delivered_time" +
    " ,oot.sync_time as oot_sync_time" +
    " ,oot.update_time as oot_update_time" +
    " ,oot.abnormal_user_id as oot_abnormal_user_id" +
    " ,oot.process_user_id as oot_process_user_id" +
    " ,oot.pack_user_id as oot_pack_user_id" +
    " ,oot.import_user_id as oot_import_user_id" +
    " ,oot.ship_user_id as oot_ship_user_id" +
    " ,oot.delivered_user_id as oot_delivered_user_id" +
    " ,oot.cutoff_user_id as oot_cutoff_user_id" +
    " ,oot.cutoff_finish_user_id as oot_cutoff_finish_user_id" +
    " ,oot.oot_timestamp as oot_timestamp" +
    " ,null as oot_abnormal_deal_time" +
    " ,null as oot_abnormal_deal_user_id" +
    " ,oot.ascan_time as oot_ascan_time" +
    " FROM (select * from ods.zy_wms_order_operation_time where data_flag<>'delete' ) as oot" +
    " left join (select * from ods.zy_wms_orders where data_flag<>'delete' ) as od on od.order_id=oot.order_id"
  def main(args: Array[String]): Unit = {
    Dw_fact_common.getRunCode_hive_nopartition_full_Into(task, tableName, Array(gc_wms,zy_wms),Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)
  }
}
