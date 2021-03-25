package com.zongteng.ztetl.etl.dw.fact.full.receiving

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_quality_control_full {
  //任务名称(一般同类名)
  private val task = "Dw_fact_quality_control_full"

  //dw层类名
  private val tableName = "fact_quality_control"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms ="SELECT qc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,qc.datasource_num_id as datasource_num_id" +
    " ,qc.data_flag as data_flag" +
    " ,qc.integration_id as integration_id" +
    " ,qc.created_on_dt as created_on_dt" +
    " ,qc.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(qc.datasource_num_id,qc.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(qc.datasource_num_id,qc.product_id) as bigint) as product_key" +
    " ,cast(concat(qc.datasource_num_id,qc.customer_id) as bigint) as customer_key" +
    " ,qc.qc_id as qc_id" +
    " ,qc.qc_code as qc_code" +
    " ,qc.reference_no as qc_reference_no" +
    " ,qc.rd_id as qc_rd_id" +
    " ,qc.receiving_id as qc_receiving_id" +
    " ,qc.warehouse_id as qc_warehouse_id" +
    " ,qc.receiving_code as qc_receiving_code" +
    " ,qc.product_barcode as qc_product_barcode" +
    " ,qc.qc_type as qc_type" +
    " ,nvl(pvl1.vl_bi_name,qc.qc_type) as qc_type_val" +
    " ,qc.product_id as qc_product_id" +
    " ,qc.customer_code as qc_customer_code" +
    " ,qc.customer_id as qc_customer_id" +
    " ,qc.receiving_user_id as qc_receiving_user_id" +
    " ,qc.qc_operator_id as qc_operator_id" +
    " ,qc.qc_quantity as qc_quantity" +
    " ,qc.qc_received_quantity as qc_received_quantity" +
    " ,qc.qc_quantity_sellable as qc_quantity_sellable" +
    " ,qc.qc_quantity_unsellable as qc_quantity_unsellable" +
    " ,qc.qc_delivery_quantity as qc_delivery_quantity" +
    " ,qc.qc_status as qc_status" +
    " ,nvl(pvl2.vl_bi_name,qc.qc_status) as qc_status_val" +
    " ,qc.qc_abnormal_status as qc_abnormal_status" +
    " ,nvl(pvl3.vl_bi_name,qc.qc_abnormal_status) as qc_abnormal_status_val" +
    " ,qc.lc_code as qc_lc_code" +
    " ,qc.qc_note as qc_note" +
    " ,qc.qc_finish_time as qc_finish_time" +
    " ,qc.qc_add_time as qc_add_time" +
    " ,qc.qc_update_time as qc_update_time" +
    " ,qc.qc_timestamp as qc_timestamp" +
    " ,date_format(qc.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.gc_wms_quality_control where data_flag<>'delete' ) as qc" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='qc_type' and pvl.vl_datasource_table='gc_wms_quality_control') as pvl1 on pvl1.vl_value=qc.qc_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='qc_status' and pvl.vl_datasource_table='gc_wms_quality_control') as pvl2 on pvl2.vl_value=qc.qc_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='qc_abnormal_status' and pvl.vl_datasource_table='gc_wms_quality_control') as pvl3 on pvl3.vl_value=qc.qc_abnormal_status"
  private val zy_wms ="SELECT qc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,qc.datasource_num_id as datasource_num_id" +
    " ,qc.data_flag as data_flag" +
    " ,qc.integration_id as integration_id" +
    " ,qc.created_on_dt as created_on_dt" +
    " ,qc.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(qc.datasource_num_id,qc.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(qc.datasource_num_id,qc.product_id) as bigint) as product_key" +
    " ,cast(concat(qc.datasource_num_id,qc.customer_id) as bigint) as customer_key" +
    " ,qc.qc_id as qc_id" +
    " ,qc.qc_code as qc_code" +
    " ,qc.reference_no as reference_no" +
    " ,qc.rd_id as rd_id" +
    " ,qc.receiving_id as receiving_id" +
    " ,qc.warehouse_id as warehouse_id" +
    " ,qc.receiving_code as receiving_code" +
    " ,qc.product_barcode as product_barcode" +
    " ,qc.qc_type as qc_type" +
    " ,nvl(pvl1.vl_bi_name,qc.qc_type) as qc_type_val" +
    " ,qc.product_id as product_id" +
    " ,qc.customer_code as customer_code" +
    " ,qc.customer_id as customer_id" +
    " ,qc.receiving_user_id as receiving_user_id" +
    " ,qc.qc_operator_id as qc_operator_id" +
    " ,qc.qc_quantity as qc_quantity" +
    " ,qc.qc_received_quantity as qc_received_quantity" +
    " ,qc.qc_quantity_sellable as qc_quantity_sellable" +
    " ,qc.qc_quantity_unsellable as qc_quantity_unsellable" +
    " ,qc.qc_delivery_quantity as qc_delivery_quantity" +
    " ,qc.qc_status as qc_status" +
    " ,nvl(pvl2.vl_bi_name,qc.qc_status) as qc_status_val" +
    " ,qc.qc_abnormal_status as qc_abnormal_status" +
    " ,nvl(pvl3.vl_bi_name,qc.qc_abnormal_status) as qc_abnormal_status_val" +
    " ,qc.lc_code as lc_code" +
    " ,qc.qc_note as qc_note" +
    " ,qc.qc_finish_time as qc_finish_time" +
    " ,qc.qc_add_time as qc_add_time" +
    " ,qc.qc_update_time as qc_update_time" +
    " ,qc.qc_timestamp as qc_timestamp" +
    " ,date_format(qc.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.zy_wms_quality_control where data_flag<>'delete' ) as qc" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='qc_type' and pvl.vl_datasource_table='zy_wms_quality_control') as pvl1 on pvl1.vl_value=qc.qc_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='qc_status' and pvl.vl_datasource_table='zy_wms_quality_control') as pvl2 on pvl2.vl_value=qc.qc_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='qc_abnormal_status' and pvl.vl_datasource_table='zy_wms_quality_control') as pvl3 on pvl3.vl_value=qc.qc_abnormal_status"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms,zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))

    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqlArray, Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))

  }
}
