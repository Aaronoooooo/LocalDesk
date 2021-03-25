package com.zongteng.ztetl.etl.dw.fact.full.orders

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_order_product_full {

  // 任务名称(一般同类名)
  private val task = "Dw_fact_order_product_full"

  // dw层类名
  private val tableName = "fact_order_product"

  // 获取当天的时间（yyyyMMdd）
  private val nowDate: String = DateUtil.getNowTime()

  private val gc_wms = "SELECT\n" +
    "    CONCAT(op.datasource_num_id, op.op_id) as row_wid,\n" +
    "    cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMMdd' ) AS string ) AS etl_proc_wid,\n" +
    "    current_timestamp ( ) as w_insert_dt,\n" +
    "    current_timestamp ( ) as w_update_dt,\n" +
    "    op.datasource_num_id as datasource_num_id,\n" +
    "    op.data_flag as data_flag,\n" +
    "    op.integration_id as integration_id,\n" +
    "    op.created_on_dt as created_on_dt,\n" +
    "    op.changed_on_dt as changed_on_dt,\n" +
    "\n" +
    "    0 AS timezone,\n" +
    "    0.00 AS exchange_rate,\n" +
    "\n" +
    "    CONCAT(op.datasource_num_id, op.order_id) as order_key,\n" +
    "    CONCAT(op.datasource_num_id, od.customer_id) as customer_key,\n" +
    "    CONCAT(op.datasource_num_id, od.warehouse_id) as warehouse_key,\n" +
    "    CONCAT(op.datasource_num_id, od.to_warehouse_id) as to_warehouse_key,\n" +
    "    CONCAT(op.datasource_num_id, sm.sm_id) as sm_key,\n" +
    "    CONCAT(op.datasource_num_id, op.product_id) as product_key,\n" +
    "\n" +
    "    op.op_id as op_id,\n" +
    "    op.order_id as op_order_id,\n" +
    "    op.order_code as op_order_code ,\n" +
    "    op.product_id as op_product_id,\n" +
    "    op.product_barcode  as op_product_barcode ,\n" +
    "    op.op_quantity as op_quantity,\n" +
    "    op.op_final_value_fee as op_final_value_fee,\n" +
    "    op.op_paypal_fee as op_paypal_fee,\n" +
    "    op.op_sales_price as op_sales_price,\n" +
    "    op.op_category as op_category,\n" +
    "    op.op_recv_account  as op_recv_account ,\n" +
    "    op.op_ref_tnx as op_ref_tnx,\n" +
    "    op.op_ref_item_id as op_ref_item_id,\n" +
    "    op.op_ref_buyer_id  as op_ref_buyer_id,\n" +
    "    op.op_record_id as op_record_id,\n" +
    "    op.op_ref_paydate as op_ref_paydate,\n" +
    "    op.op_add_time as op_add_time,\n" +
    "    op.op_update_time as op_update_time,\n" +
    "    op.shared_sign as op_shared_sign,\n" +
    "    op.shared_unit_price as op_shared_unit_price,\n" +
    "    op.fba_product_code as op_fba_product_code ,\n" +
    "    op.order_product_declared_value as op_order_product_declared_value,\n" +
    "    op.op_length as op_length,\n" +
    "    op.op_width as op_width,\n" +
    "    op.op_height as op_height,\n" +
    "    op.op_weight as op_weight,\n" +
    "    op.op_timestamp as op_timestamp,\n" +
    "    op.sc_declared_value as op_sc_declared_value,\n" +
    "    date_format(op.op_add_time, 'yyyyMM') as month\n" +
    "FROM (SELECT * FROM ods.gc_wms_order_product WHERE data_flag != 'DELETE') op\n" +
    "LEFT JOIN (SELECT * FROM ods.gc_wms_orders WHERE data_flag != 'DELETE') od ON op.order_id = od.order_id\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("gc_wms_shipping_method","sm")} ON od.sm_code = sm.sm_code\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("gc_wms_warehouse","wa")} ON od.warehouse_id = wa.warehouse_id"


  private val zy_wms = "SELECT\n" +
    "    CONCAT(op.datasource_num_id, op.op_id) as row_wid,\n" +
    "    cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMMdd' ) AS string ) AS etl_proc_wid,\n" +
    "    current_timestamp ( ) as w_insert_dt,\n" +
    "    current_timestamp ( ) as w_update_dt,\n" +
    "    op.datasource_num_id as datasource_num_id,\n" +
    "    op.data_flag as data_flag,\n" +
    "    op.integration_id as integration_id,\n" +
    "    op.created_on_dt as created_on_dt,\n" +
    "    op.changed_on_dt as changed_on_dt,\n" +
    "\n" +
    "    0 AS timezone ,\n" +
    "    0.00 AS exchange_rate,\n" +
    "\n" +
    "    CONCAT(op.datasource_num_id, op.order_id) as order_key,\n" +
    "    CONCAT(op.datasource_num_id, od.customer_id) as customer_key,\n" +
    "    CONCAT(op.datasource_num_id, od.warehouse_id) as warehouse_key,\n" +
    "    CONCAT(op.datasource_num_id, od.to_warehouse_id) as to_warehouse_key,\n" +
    "    CONCAT(op.datasource_num_id, sm.sm_id) as sm_key,\n" +
    "    CONCAT(op.datasource_num_id, op.product_id) as product_key,\n" +
    "\n" +
    "    op.op_id as op_id,\n" +
    "    op.order_id as op_order_id,\n" +
    "    op.order_code as op_order_code ,\n" +
    "    op.product_id as op_product_id,\n" +
    "    op.product_barcode  as op_product_barcode ,\n" +
    "    op.op_quantity as op_quantity,\n" +
    "    op.op_final_value_fee as op_final_value_fee,\n" +
    "    op.op_paypal_fee as op_paypal_fee,\n" +
    "    op.op_sales_price as op_sales_price,\n" +
    "    op.op_category as op_category,\n" +
    "    op.op_recv_account  as op_recv_account ,\n" +
    "    op.op_ref_tnx as op_ref_tnx,\n" +
    "    op.op_ref_item_id as op_ref_item_id,\n" +
    "    op.op_ref_buyer_id  as op_ref_buyer_id,\n" +
    "    op.op_record_id as op_record_id,\n" +
    "    op.op_ref_paydate as op_ref_paydate,\n" +
    "    op.op_add_time as op_add_time,\n" +
    "    op.op_update_time as op_update_time,\n" +
    "    op.shared_sign as op_shared_sign,\n" +
    "    op.shared_unit_price as op_shared_unit_price,\n" +
    "    op.fba_product_code as op_fba_product_code ,\n" +
    "    op.order_product_declared_value as op_order_product_declared_value,\n" +
    "    op.op_length as op_length,\n" +
    "    op.op_width as op_width,\n" +
    "    op.op_height as op_height,\n" +
    "    op.op_weight as op_weight,\n" +
    "    op.op_timestamp as op_timestamp,\n" +
    "    null as op_sc_declared_value,\n" +
    "    date_format(op.op_add_time, 'yyyyMM') as month\n" +
    "FROM (SELECT * FROM ods.zy_wms_order_product WHERE data_flag != 'DELETE') op\n" +
    "LEFT JOIN (SELECT * FROM ods.zy_wms_orders WHERE data_flag != 'DELETE') od ON op.order_id = od.order_id\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("zy_wms_shipping_method","sm")} ON od.sm_code = sm.sm_code\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("zy_wms_warehouse","wa")} ON od.warehouse_id = wa.warehouse_id"


  def main(args: Array[String]): Unit = {
    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, Array(gc_wms, zy_wms))
  }


}
