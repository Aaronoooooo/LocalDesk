package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}

object DwFactAdvancePickingDetailFull {

  // 任务名称
  val task = "Spark_Etl_Dw_Fact_Advance_Picking_Detail"

  // dw层事实表表名
  val tableName = "fact_advance_picking_detail"

  val gc_wms = "SELECT \n" +
    "  adp.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  adp.datasource_num_id AS datasource_num_id,\n" +
    "  adp.data_flag AS data_flag,\n" +
    "  adp.apd_id AS integration_id,\n" +
    "  adp.created_on_dt AS created_on_dt,\n" +
    "  adp.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "\n" +
    "  concat(adp.datasource_num_id, adp.warehouse_id) AS warehouse_key,\n" +
    "  concat(adp.datasource_num_id, adp.product_id) AS product_key,\n" +
    "\n" +
    "  adp.apd_id AS apd_id,\n" +
    "  adp.warehouse_id AS apd_warehouse_id,\n" +
    "  adp.order_id AS apd_order_id,\n" +
    "  adp.order_code AS apd_order_code,\n" +
    "  adp.pd_status AS apd_pd_status,\n" +
    "  nvl(v_pd_status.vl_name, adp.pd_status) AS apd_pd_status_val,\n" +
    "  adp.pc_id AS apd_pc_id,\n" +
    "  adp.op_id AS apd_op_id,\n" +
    "  adp.product_id AS apd_product_id,\n" +
    "  adp.product_barcode AS apd_product_barcode,\n" +
    "  adp.pd_quantity AS apd_pd_quantity,\n" +
    "  adp.ibo_id AS apd_ibo_id,\n" +
    "  adp.ib_id AS apd_ib_id,\n" +
    "  adp.pv_id AS apd_pv_id,\n" +
    "  adp.pick_sort AS apd_pick_sort,\n" +
    "  adp.pick_point AS apd_pick_point,\n" +
    "  adp.wa_code AS apd_wa_code,\n" +
    "  adp.lc_code AS apd_lc_code,\n" +
    "  adp.receiving_id AS apd_receiving_id,\n" +
    "  adp.receiving_code AS apd_receiving_code,\n" +
    "  adp.po_code AS apd_po_code,\n" +
    "  adp.pd_add_time AS apd_pd_add_time,\n" +
    "  adp.is_flow_volume AS apd_is_flow_volume,\n" +
    "  nvl(v_is_flow_volume.vl_name, adp.is_flow_volume) AS apd_is_flow_volume_val,\n" +
    "  date_format(adp.pd_add_time, 'yyyyMM') AS month\n" +
    "FROM ods.gc_wms_advance_picking_detail adp\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
    "  WHERE vl_type = 'pd_status' AND vl_datasource_table = 'gc_wms_advance_picking_detail') v_pd_status\n" +
    "ON adp.pd_status = v_pd_status.vl_value\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
    "  WHERE vl_type = 'is_flow_volume' AND vl_datasource_table = 'gc_wms_advance_picking_detail') v_is_flow_volume\n" +
    "ON adp.is_flow_volume = v_is_flow_volume.vl_value"

 val zy_wms = "SELECT \n" +
   "  adp.row_wid AS row_wid,\n" +
   "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
   "  current_timestamp ( ) AS w_insert_dt,\n" +
   "  current_timestamp ( ) AS w_update_dt,\n" +
   "  adp.datasource_num_id AS datasource_num_id,\n" +
   "  adp.data_flag AS data_flag,\n" +
   "  adp.apd_id AS integration_id,\n" +
   "  adp.created_on_dt AS created_on_dt,\n" +
   "  adp.changed_on_dt AS changed_on_dt,\n" +
   "  NULL AS timezone,\n" +
   "  NULL AS exchange_rate,\n" +
   "\n" +
   "  concat(adp.datasource_num_id, adp.warehouse_id) AS warehouse_key,\n" +
   "  concat(adp.datasource_num_id, adp.product_id) AS product_key,\n" +
   "\n" +
   "  adp.apd_id AS apd_id,\n" +
   "  adp.warehouse_id AS apd_warehouse_id,\n" +
   "  adp.order_id AS apd_order_id,\n" +
   "  adp.order_code AS apd_order_code,\n" +
   "  adp.pd_status AS apd_pd_status,\n" +
   "  nvl(v_pd_status.vl_name, adp.pd_status) AS apd_pd_status_val,\n" +
   "  adp.pc_id AS apd_pc_id,\n" +
   "  adp.op_id AS apd_op_id,\n" +
   "  adp.product_id AS apd_product_id,\n" +
   "  adp.product_barcode AS apd_product_barcode,\n" +
   "  adp.pd_quantity AS apd_pd_quantity,\n" +
   "  adp.ibo_id AS apd_ibo_id,\n" +
   "  adp.ib_id AS apd_ib_id,\n" +
   "  adp.pv_id AS apd_pv_id,\n" +
   "  adp.pick_sort AS apd_pick_sort,\n" +
   "  adp.pick_point AS apd_pick_point,\n" +
   "  adp.wa_code AS apd_wa_code,\n" +
   "  adp.lc_code AS apd_lc_code,\n" +
   "  adp.receiving_id AS apd_receiving_id,\n" +
   "  adp.receiving_code AS apd_receiving_code,\n" +
   "  adp.po_code AS apd_po_code,\n" +
   "  adp.pd_add_time AS apd_pd_add_time,\n" +
   "  NULL AS apd_is_flow_volume,\n" +
   "  NULL AS apd_is_flow_volume_val,\n" +
   "  date_format(adp.pd_add_time, 'yyyyMM') AS month\n" +
   "FROM (SELECT * FROM ods.zy_wms_advance_picking_detail WHERE data_flag != 'DELETE') adp\n" +
   "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
   "  WHERE vl_type = 'pd_status' AND vl_datasource_table = 'zy_wms_advance_picking_detail') v_pd_status\n" +
   "ON adp.pd_status = v_pd_status.vl_value"

  val systemCodes = Array(
    SystemCodeUtil.GC_WMS,
    SystemCodeUtil.ZY_WMS
  )


  def main(args: Array[String]): Unit = {

    val sqlArray: Array[String] = Array(gc_wms, zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))

    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqlArray, systemCodes)
  }

}
