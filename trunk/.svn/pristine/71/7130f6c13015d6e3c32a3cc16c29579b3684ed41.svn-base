package com.zongteng.ztetl.etl.dw.fact.full.receiving

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.etl.dw.fact.full.receiving.Dw_fact_putaway_detail_full.{gc_wms_gc, tableName, task, zy_wms}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_receiving_detail_full {
  //任务名称(一般同类名)
  private val task = "Dw_fact_receiving_detail_full"

  //dw层类名
  private val tableName = "fact_receiving_detail"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_oms = "SELECT rvd.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,rvd.datasource_num_id as datasource_num_id" +
    " ,rvd.data_flag as data_flag" +
    " ,rvd.integration_id as integration_id" +
    " ,rvd.created_on_dt as  created_on_dt" +
    " ,rvd.changed_on_dt as  changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(rvd.datasource_num_id,rvd.product_id) as bigint) as product_key" +
    " ,cast(concat(rv.datasource_num_id,rv.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(rv.datasource_num_id,rv.warehouse_id) as bigint) as to_warehouse_key" +
    " ,cast(concat(rv.datasource_num_id,rv.transit_warehouse_id) as bigint) as transit_warehouse_key" +
    " ,cast(concat(rv.datasource_num_id,rv.customer_id) as bigint) as customer_key" +
    " ,rvd.rd_id as rd_id" +
    " ,rvd.rdc_id as rd_rdc_id" +
    " ,rvd.supplier_id as rd_supplier_id" +
    " ,rvd.receiving_id as rd_receiving_id" +
    " ,rvd.receiving_code as rd_receiving_code" +
    " ,rvd.sm_code as rd_sm_code" +
    " ,rvd.rd_transfer_status as rd_transfer_status" +
    " ,rvd.receiving_line_no as rd_receiving_line_no" +
    " ,rvd.rd_status as rd_status" +
    " ,nvl(pvl4.vl_bi_name, rvd.rd_status) as rd_status_val" +
    " ,rvd.receiving_exception as rd_receiving_exception" +
    " ,rvd.receiving_exception_handle as rd_receiving_exception_handle" +
    " ,rvd.exception_process_instruction as rd_exception_process_instruction" +
    " ,rvd.product_id as rd_product_id" +
    " ,rvd.product_sku as rd_product_sku" +
    " ,rvd.product_barcode as rd_product_barcode" +
    " ,rv.warehouse_code as rd_warehouse_code" +
    " ,rv.warehouse_code as rd_to_warehouse_code" +
    " ,rv.transit_warehouse_code as rd_transit_warehouse_code" +
    " ,rv.customer_code as rd_customer_code" +
    " ,rvd.rd_receiving_qty as rd_receiving_qty" +
    " ,null as rd_rb_add_quantity" +
    " ,rvd.rd_putaway_qty as rd_putaway_qty" +
    " ,rvd.rd_received_qty as rd_received_qty" +
    " ,null as rd_delivery_qty" +
    " ,rvd.is_qc as rd_is_qc" +
    " ,nvl(pvl1.vl_bi_name,rvd.is_qc) as rd_is_qc_val" +
    " ,rvd.is_priority as rd_is_priority" +
    " ,nvl(pvl2.vl_bi_name,rvd.is_priority) as rd_is_priority_val" +
    " ,rvd.is_need_dev_photo as rd_is_need_dev_photo" +
    " ,rvd.is_need_dev_desc as rd_is_need_dev_desc" +
    " ,rvd.rd_note as rd_note" +
    " ,rvd.product_package_type as rd_product_package_type" +
    " ,rvd.box_no as rd_box_no" +
    " ,rvd.package_type as rd_package_type" +
    " ,rvd.value_added_type as rd_value_added_type" +
    " ,rvd.line_weight as rd_line_weight" +
    " ,rvd.fba_product_code as rd_fba_product_code" +
    " ,rvd.reference_box_no as rd_reference_box_no" +
    " ,rvd.is_new_add as rd_is_new_add" +
    " ,nvl(pvl3.vl_bi_name,rvd.is_new_add) as rd_is_new_add_val" +
    " ,rvd.box_number as rd_box_number" +
    " ,rvd.add_new_box_quantity as rd_add_new_box_quantity" +
    " ,rvd.valid_date as rd_valid_date" +
    " ,rvd.rd_timestamp as rd_timestamp" +
    " ,rvd.rd_add_time as rd_add_time" +
    " ,rvd.rd_update_time as rd_update_time" +
    " ,date_format(rvd.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.gc_oms_receiving_detail where data_flag<>'delete' ) as rvd" +
    " left join (select * from ods.gc_oms_receiving where data_flag<>'delete' )  as rv on rv.receiving_id=rvd.receiving_id" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='is_qc' and pvl.vl_datasource_table='gc_oms_receiving_detail') as pvl1 on pvl1.vl_value=rvd.is_qc" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='is_priority' and pvl.vl_datasource_table='gc_oms_receiving_detail') as pvl2 on pvl2.vl_value=rvd.is_priority" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='is_new_add' and pvl.vl_datasource_table='gc_oms_receiving_detail') as pvl3 on pvl3.vl_value=rvd.is_new_add" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='rd_status' and pvl.vl_datasource_table='gc_oms_receiving_detail') as pvl4 on pvl4.vl_value=rvd.rd_status"
  private val gc_wms_gc = "SELECT rvd.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,rvd.datasource_num_id as datasource_num_id" +
    " ,rvd.data_flag as data_flag" +
    " ,rvd.integration_id as integration_id" +
    " ,rvd.created_on_dt as  created_on_dt" +
    " ,rvd.changed_on_dt as  changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(pro.datasource_num_id,pro.product_id) as bigint) as product_key" +
    " ,cast(concat(wh.datasource_num_id,wh.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(wh.datasource_num_id,wh.warehouse_id) as bigint) as to_warehouse_key" +
    " ,null as transit_warehouse_key" +
    " ,cast(concat(cus.datasource_num_id,cus.customer_id) as bigint) as customer_key" +
    " ,rvd.rd_id as rd_id" +
    " ,null as rd_rdc_id" +
    " ,null as rd_supplier_id" +
    " ,rvd.receiving_id as rd_receiving_id" +
    " ,rvd.receiving_code as rd_receiving_code" +
    " ,null as rd_sm_code" +
    " ,null as rd_transfer_status" +
    " ,null as rd_receiving_line_no" +
    " ,rvd.rd_status as rd_status" +
    " ,nvl(pvl4.vl_bi_name, rvd.rd_status) as rd_status_val" +
    " ,null as rd_receiving_exception" +
    " ,null as rd_receiving_exception_handle" +
    " ,null as rd_exception_process_instruction" +
    " ,null as rd_product_id" +
    " ,null as rd_product_sku" +
    " ,rvd.product_barcode as rd_product_barcode" +
    " ,rv.warehouse_code as rd_warehouse_code" +
    " ,rv.warehouse_code as rd_to_warehouse_code" +
    " ,null as rd_transit_warehouse_code" +
    " ,rv.customer_code as rd_customer_code" +
    " ,rvd.rd_receiving_qty as rd_receiving_qty" +
    " ,rvd.rb_add_quantity as rd_rb_add_quantity" +
    " ,rvd.rd_putaway_qty as rd_putaway_qty" +
    " ,rvd.rd_received_qty as rd_received_qty" +
    " ,null as rd_delivery_qty" +
    " ,null as rd_is_qc" +
    " ,null as rd_is_qc_val" +
    " ,null as rd_is_priority" +
    " ,null as rd_is_priority_val" +
    " ,null as rd_is_need_dev_photo" +
    " ,null as rd_is_need_dev_desc" +
    " ,null as rd_note" +
    " ,null as rd_product_package_type" +
    " ,null as rd_box_no" +
    " ,null as rd_package_type" +
    " ,null as rd_value_added_type" +
    " ,null as rd_line_weight" +
    " ,rvd.fba_product_code as rd_fba_product_code" +
    " ,null as rd_reference_box_no" +
    " ,rvd.is_new_add as rd_is_new_add" +
    " ,nvl(pvl3.vl_bi_name,rvd.is_new_add) as rd_is_new_add_val" +
    " ,null as rd_box_number" +
    " ,null as rd_add_new_box_quantity" +
    " ,null as rd_valid_date" +
    " ,rvd.r_timestamp as rd_timestamp" +
    " ,rvd.rd_add_time as rd_add_time" +
    " ,rvd.rd_update_time as rd_update_time" +
    " ,date_format(rvd.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.gc_wms_gc_receiving_detail where data_flag<>'delete' ) as rvd" +
    s" left join ${Dw_dim_common.getDimSql("gc_wms_product","pro")} on pro.product_barcode=rvd.product_barcode" +
    " left join (select * from ods.gc_wms_gc_receiving where data_flag<>'delete' )  as rv on rv.receiving_id=rvd.receiving_id" +
    s" left join ${Dw_dim_common.getDimSql("gc_wms_warehouse","wh")} on wh.warehouse_code=rv.warehouse_code" +
    s" left join ${Dw_dim_common.getDimSql("gc_wms_customer","cus")} on cus.customer_code=rv.customer_code" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='is_new_add' and pvl.vl_datasource_table='gc_wms_gc_receiving_detail') as pvl3 on pvl3.vl_value=rvd.is_new_add" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='rd_status' and pvl.vl_datasource_table='gc_wms_gc_receiving_detail') as pvl4 on pvl4.vl_value=rvd.rd_status"
  private val zy_wms = "SELECT rvd.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,rvd.datasource_num_id as datasource_num_id" +
    " ,rvd.data_flag as data_flag" +
    " ,rvd.integration_id as integration_id" +
    " ,rvd.created_on_dt as  created_on_dt" +
    " ,rvd.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(rvd.datasource_num_id,rvd.product_id) as bigint) as product_key" +
    " ,cast(concat(rv.datasource_num_id,rv.warehouse_id) as bigint) as warehouse_key" +
    " ,cast(concat(rv.datasource_num_id,rv.to_warehouse_id) as bigint) as to_warehouse_key" +
    " ,cast(concat(rv.datasource_num_id,rv.transit_warehouse_id) as bigint) as transit_warehouse_key" +
    " ,cast(concat(rv.datasource_num_id,rv.customer_id) as bigint) as customer_key" +
    " ,rvd.rd_id as rd_id" +
    " ,null as rd_rdc_id" +
    " ,null as rd_supplier_id" +
    " ,rvd.receiving_id as rd_receiving_id" +
    " ,rvd.receiving_code as rd_receiving_code" +
    " ,rvd.sm_code as rd_sm_code" +
    " ,rvd.rd_transfer_status as rd_transfer_status" +
    " ,rvd.receiving_line_no as rd_receiving_line_no" +
    " ,rvd.rd_status as rd_status" +
    " ,nvl(pvl4.vl_bi_name, rvd.rd_status) as rd_status_val" +
    " ,rvd.receiving_exception as rd_receiving_exception" +
    " ,rvd.receiving_exception_handle as rd_receiving_exception_handle" +
    " ,rvd.exception_process_instruction as rd_exception_process_instruction" +
    " ,rvd.product_id as rd_product_id" +
    " ,null as rd_product_sku" +
    " ,rvd.product_barcode as rd_product_barcode" +
    " ,wa1.warehouse_code as rd_warehouse_code" +
    " ,wa2.warehouse_code as rd_to_warehouse_code" +
    " ,wa3.warehouse_code as rd_transit_warehouse_code" +
    " ,rv.customer_code as rd_customer_code" +
    " ,rvd.rd_receiving_qty as rd_receiving_qty" +
    " ,null as rd_rb_add_quantity" +
    " ,rvd.rd_putaway_qty as rd_putaway_qty" +
    " ,rvd.rd_received_qty as rd_received_qty" +
    " ,rvd.rd_delivery_qty as rd_delivery_qty" +
    " ,rvd.is_qc as rd_is_qc" +
    " ,nvl(pvl1.vl_bi_name,rvd.is_qc) as rd_is_qc_val" +
    " ,rvd.is_priority as rd_is_priority" +
    " ,nvl(pvl2.vl_bi_name, rvd.is_priority) as rd_is_priority_val" +
    " ,rvd.is_need_dev_photo as rd_is_need_dev_photo" +
    " ,rvd.is_need_dev_desc as rd_is_need_dev_desc" +
    " ,rvd.rd_note as rd_note" +
    " ,rvd.product_package_type as rd_product_package_type" +
    " ,null as rd_box_no" +
    " ,null as rd_package_type" +
    " ,rvd.value_added_type as rd_value_added_type" +
    " ,null as rd_line_weight" +
    " ,rvd.fba_product_code as rd_fba_product_code" +
    " ,rvd.reference_box_no as rd_reference_box_no" +
    " ,rvd.is_new_add as rd_is_new_add" +
    " ,nvl(pvl3.vl_bi_name,rvd.is_new_add) as rd_is_new_add_val" +
    " ,null as rd_box_number" +
    " ,null as rd_add_new_box_quantity" +
    " ,rvd.valid_date as rd_valid_date" +
    " ,rvd.rd_timestamp as rd_timestamp" +
    " ,rvd.rd_add_time as rd_add_time" +
    " ,rvd.rd_update_time as rd_update_time" +
    " ,date_format(rvd.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.zy_wms_receiving_detail where data_flag<>'delete' ) as rvd" +
    " left join (select * from ods.zy_wms_receiving where data_flag<>'delete' )  as rv on rv.receiving_id=rvd.receiving_id" +
    s" left join ${Dw_dim_common.getDimSql("zy_wms_warehouse","wa1")} on wa1.warehouse_id=rv.warehouse_id" +
    s" left join ${Dw_dim_common.getDimSql("zy_wms_warehouse","wa2")} on wa2.warehouse_id=rv.to_warehouse_id" +
    s" left join ${Dw_dim_common.getDimSql("zy_wms_warehouse","wa3")} on wa3.warehouse_id=rv.transit_warehouse_id" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='is_qc' and pvl.vl_datasource_table='zy_wms_receiving_detail') as pvl1 on pvl1.vl_value=rvd.is_qc" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='is_priority' and pvl.vl_datasource_table='zy_wms_receiving_detail') as pvl2 on pvl2.vl_value=rvd.is_priority" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='is_new_add' and pvl.vl_datasource_table='zy_wms_receiving_detail') as pvl3 on pvl3.vl_value=rvd.is_new_add" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='rd_status' and pvl.vl_datasource_table='zy_wms_receiving_detail') as pvl4 on pvl4.vl_value=rvd.rd_status"



  def main(args: Array[String]): Unit = {

    val sqlArray: Array[String] = Array(gc_oms,gc_wms_gc,zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))

    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqlArray, Array(SystemCodeUtil.GC_OMS,SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))

  }
}
