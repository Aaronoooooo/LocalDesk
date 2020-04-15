package com.zongteng.ztetl.etl.dw.fact.full.orders

import com.zongteng.ztetl.common.Dw_fact_common
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_order_product_physical_full {

  // 任务名称(一般同类名)
  private val task = "Dw_fact_order_product_physical_full"

  // dw层类名
  private val tableName = "fact_order_product_physical"

  // 获取当天的时间（yyyyMMdd）
  private val nowDate: String = DateUtil.getNowTime()

  private val gc_wms = "SELECT \n" +
    "    CONCAT(oppr.datasource_num_id, oppr_id) as row_wid,\n" +
    "    cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMMdd' ) AS string ) AS etl_proc_wid,\n" +
    "    current_timestamp ( ) as w_insert_dt,\n" +
    "    current_timestamp ( ) as w_update_dt,\n" +
    "    oppr.datasource_num_id as datasource_num_id,\n" +
    "    oppr.data_flag as data_flag,\n" +
    "    oppr.integration_id as integration_id,\n" +
    "    oppr.created_on_dt as created_on_dt,\n" +
    "    oppr.changed_on_dt as changed_on_dt,\n" +
    "    0 AS timezone,\n" +
    "    0.00 AS exchange_rate,\n" +
    "\n" +
    "    CONCAT(oppr.datasource_num_id, oppr.product_id) as product_key,\n" +
    "    CONCAT(oppr.datasource_num_id, oppr.op_id) as op_key,\n" +
    "\n" +
    "    oppr.oppr_id as oppr_id,\n" +
    "    oppr.order_code as oppr_order_code,\n" +
    "    oppr.product_barcode as oppr_product_barcode,\n" +
    "    oppr.wp_code as oppr_wp_code,\n" +
    "    oppr.occupy_quantity as oppr_occupy_quantity,\n" +
    "    oppr.product_id as oppr_product_id,\n" +
    "    oppr.op_id as oppr_op_id,\n" +
    "    cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMM' ) AS string ) as month\n" +
    "FROM (SELECT * FROM ods.gc_wms_order_product_physical_relation WHERE data_flag != 'DELETE') AS oppr"

  def main(args: Array[String]): Unit = {
    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, Array(gc_wms))
  }
}
