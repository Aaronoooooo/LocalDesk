package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}

object DwFactPickingDetailFull {

  // 任务名称
  private val task = "Spark_Etl_Dw_Fact_Picking_Detail_Full"

  // dw层表名
  private val dwTable = "fact_picking_detail"


  def getGcOwmsSql = {

    val gc_owms = "SELECT \n" +
      "  pd.row_wid AS row_wid,\n" +
      "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
      "  current_timestamp ( ) AS w_insert_dt,\n" +
      "  current_timestamp ( ) AS w_update_dt,\n" +
      "  pd.datasource_num_id AS datasource_num_id,\n" +
      "  pd.data_flag AS data_flag,\n" +
      "  pd.pd_id AS integration_id,\n" +
      "  pd.created_on_dt AS created_on_dt,\n" +
      "  pd.changed_on_dt AS changed_on_dt,\n" +
      "  NULL AS timezone,\n" +
      "  NULL AS exchange_rate,\n" +
      "\n" +
      "  concat(pd.datasource_num_id, pd.pd_id) AS product_key,\n" +
      "\n" +
      "  pd.pd_id AS pd_id,\n" +
      "  pd.picking_id AS pd_picking_id,\n" +
      "  pd.picking_code AS pd_picking_code,\n" +
      "  pd.order_id AS pd_order_id,\n" +
      "  pd.order_code AS pd_order_code,\n" +
      "  pd.pd_status AS pd_status,\n" +
      "  pd.op_id AS pd_op_id,\n" +
      "  pd.product_id AS pd_product_id,\n" +
      "  pd.product_barcode AS pd_product_barcode,\n" +
      "  pd.pd_quantity AS pd_quantity,\n" +
      "  pd.scan_quantity AS pd_scan_quantity,\n" +
      "  pd.ibo_id AS pd_ibo_id,\n" +
      "  pd.ib_id AS pd_ib_id,\n" +
      "  pd.pv_id AS pd_pv_id,\n" +
      "  pd.pick_point AS pd_pick_point,\n" +
      "  pd.pick_sort AS pd_pick_sort,\n" +
      "  pd.lc_code AS pd_lc_code,\n" +
      "  pd.receiving_id AS pd_receiving_id,\n" +
      "  pd.receiving_code AS pd_receiving_code,\n" +
      "  pd.po_code AS pd_po_code,\n" +
      "  pd.pd_fifo_time AS pd_fifo_time,\n" +
      "  pd.pd_add_time AS pd_add_time,\n" +
      "  pd.pd_update_time AS pd_update_time,\n" +
      "  pd.is_flow_volume AS pd_is_flow_volume,\n" +
      "  NVL(v_is_flow_volume.vl_name, pd.is_flow_volume) AS pd_is_flow_volume_val,\n" +
      "  pd.sorting_sort AS pd_sorting_sort,\n" +
      "  pd.c_code AS pd_c_code,\n" +
      "  pd.is_abnormal AS pd_is_abnormal,\n" +
      "  NVL(v_is_abnormal.vl_name, pd.is_abnormal) AS pd_is_abnormal_val,\n" +
      "  pd.pick_quantity AS pd_pick_quantity,\n" +
      "  pd.pick_status AS pd_pick_status,\n" +
      "  NVL(v_pick_status.vl_name, pd.pick_status) AS pd_pick_status_val,\n" +
      "  pd.basket_no AS pd_basket_no,\n" +
      "  pd.wa_code AS pd_wa_code,\n" +
      "  pd.is_supplement AS pd_is_supplement,\n" +
      "  NVL(v_is_supplement.vl_name, pd.is_supplement) AS pd_is_supplement_val,\n" +
      "  pd.sync_status AS pd_sync_status,\n" +
      "  NVL(v_sync_status.vl_name, pd.sync_status) AS pd_sync_status_val,\n" +
      "  pd.sync_error_message AS pd_sync_error_message,\n" +
      "  pd.abnormal_type AS pd_abnormal_type,\n" +
      "  NVL(v_abnormal_type.vl_name, pd.abnormal_type) AS pd_abnormal_type_val,\n" +
      "  pd.order_exception_sub_type AS pd_order_exception_sub_type,\n" +
      "  NVL(v_order_exception_sub_type.vl_name, pd.order_exception_sub_type) AS pd_order_exception_sub_type_val,\n" +
      "  date_format(pd.pd_add_time, 'yyyyMM') month\n" +
      "FROM ods.gc_owms_picking_detail_talbe pd\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'is_flow_volume' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_is_flow_volume ON pd.is_flow_volume = v_is_flow_volume.vl_value   \n" +
      "\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'is_abnormal' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_is_abnormal ON pd.is_abnormal = v_is_abnormal.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'pick_status' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_pick_status ON pd.pick_status = v_pick_status.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'is_supplement' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_is_supplement ON pd.is_supplement = v_is_supplement.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'sync_status' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_sync_status ON pd.sync_status = v_sync_status.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'abnormal_type' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_abnormal_type ON pd.abnormal_type = v_abnormal_type.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "WHERE vl_type = 'order_exception_sub_type' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
      ") AS v_order_exception_sub_type ON pd.order_exception_sub_type = v_order_exception_sub_type.vl_value"

    // 14个表
    val odsTable = Array(
      "gc_owms_usea_picking_detail",
    "gc_owms_uswe_picking_detail",
    "gc_owms_au_picking_detail",
    "gc_owms_cz_picking_detail",
    "gc_owms_de_picking_detail",
    "gc_owms_es_picking_detail",
    "gc_owms_frvi_picking_detail",
    "gc_owms_it_picking_detail",
    "gc_owms_jp_picking_detail",
    "gc_owms_uk_picking_detail",
    "gc_owms_ukob_picking_detail",
    "gc_owms_usnb_picking_detail",
    "gc_owms_usot_picking_detail",
    "gc_owms_ussc_picking_detail"
    )

    odsTable.map(gc_owms.replace("gc_owms_picking_detail_talbe", _))
  }

  def getZyOwmsSql = {

    val zy_owms = "SELECT \n" +
      "  pd.row_wid AS row_wid,\n" +
      "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
      "  current_timestamp ( ) AS w_insert_dt,\n" +
      "  current_timestamp ( ) AS w_update_dt,\n" +
      "  pd.datasource_num_id AS datasource_num_id,\n" +
      "  pd.data_flag AS data_flag,\n" +
      "  pd.pd_id AS integration_id,\n" +
      "  pd.created_on_dt AS created_on_dt,\n" +
      "  pd.changed_on_dt AS changed_on_dt,\n" +
      "  NULL AS timezone,\n" +
      "  NULL AS exchange_rate,\n" +
      "\n" +
      "  concat(pd.datasource_num_id, pd.pd_id) AS product_key,\n" +
      "\n" +
      "  pd.pd_id AS pd_id,\n" +
      "  pd.picking_id AS pd_picking_id,\n" +
      "  pd.picking_code AS pd_picking_code,\n" +
      "  pd.order_id AS pd_order_id,\n" +
      "  pd.order_code AS pd_order_code,\n" +
      "  pd.pd_status AS pd_status,\n" +
      "  pd.op_id AS pd_op_id,\n" +
      "  pd.product_id AS pd_product_id,\n" +
      "  pd.product_barcode AS pd_product_barcode,\n" +
      "  pd.pd_quantity AS pd_quantity,\n" +
      "  pd.scan_quantity AS pd_scan_quantity,\n" +
      "  pd.ibo_id AS pd_ibo_id,\n" +
      "  pd.ib_id AS pd_ib_id,\n" +
      "  pd.pv_id AS pd_pv_id,\n" +
      "  pd.pick_point AS pd_pick_point,\n" +
      "  pd.pick_sort AS pd_pick_sort,\n" +
      "  pd.lc_code AS pd_lc_code,\n" +
      "  pd.receiving_id AS pd_receiving_id,\n" +
      "  pd.receiving_code AS pd_receiving_code,\n" +
      "  pd.po_code AS pd_po_code,\n" +
      "  pd.pd_fifo_time AS pd_fifo_time,\n" +
      "  pd.pd_add_time AS pd_add_time,\n" +
      "  pd.pd_update_time AS pd_update_time,\n" +
      "  NULL AS pd_is_flow_volume,\n" +
      "  NULL AS pd_is_flow_volume_val,\n" +
      "  NULL AS pd_sorting_sort,\n" +
      "  NULL AS pd_c_code,\n" +
      "  NULL AS pd_is_abnormal,\n" +
      "  NULL AS pd_is_abnormal_val,\n" +
      "  NULL AS pd_pick_quantity,\n" +
      "  NULL AS pd_pick_status,\n" +
      "  NULL AS pd_pick_status_val,\n" +
      "  NULL AS pd_basket_no,\n" +
      "  NULL AS pd_wa_code,\n" +
      "  NULL AS pd_is_supplement,\n" +
      "  NULL AS pd_is_supplement_val,\n" +
      "  NULL AS pd_sync_status,\n" +
      "  NULL AS pd_sync_status_val,\n" +
      "  NULL AS pd_sync_error_message,\n" +
      "  NULL AS pd_abnormal_type,\n" +
      "  NULL AS pd_abnormal_type_val,\n" +
      "  NULL AS pd_order_exception_sub_type,\n" +
      "  NULL AS pd_order_exception_sub_type_val,\n" +
      "  date_format(pd.pd_add_time, 'yyyyMM') month\n" +
      "FROM ods.zy_owms_picking_detail_table pd"

    val odsTables = Array(
      "zy_owms_au_picking_detail",
      "zy_owms_cz_picking_detail",
      "zy_owms_de_picking_detail",
      "zy_owms_ru_picking_detail",
      "zy_owms_uk_picking_detail",
      "zy_owms_usea_picking_detail",
      "zy_owms_uswe_picking_detail",
      "zy_owms_ussc_picking_detail"
    )

    odsTables.map(zy_owms.replace("zy_owms_picking_detail_table", _))
  }

  val gc_wms = "SELECT \n" +
    "  pd.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  pd.datasource_num_id AS datasource_num_id,\n" +
    "  pd.data_flag AS data_flag,\n" +
    "  pd.pd_id AS integration_id,\n" +
    "  pd.created_on_dt AS created_on_dt,\n" +
    "  pd.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "\n" +
    "  concat(pd.datasource_num_id, pd.pd_id) AS product_key,\n" +
    "\n" +
    "  pd.pd_id AS pd_id,\n" +
    "  pd.picking_id AS pd_picking_id,\n" +
    "  pd.picking_code AS pd_picking_code,\n" +
    "  pd.order_id AS pd_order_id,\n" +
    "  pd.order_code AS pd_order_code,\n" +
    "  pd.pd_status AS pd_status,\n" +
    "  pd.op_id AS pd_op_id,\n" +
    "  pd.product_id AS pd_product_id,\n" +
    "  pd.product_barcode AS pd_product_barcode,\n" +
    "  pd.pd_quantity AS pd_quantity,\n" +
    "  pd.scan_quantity AS pd_scan_quantity,\n" +
    "  pd.ibo_id AS pd_ibo_id,\n" +
    "  pd.ib_id AS pd_ib_id,\n" +
    "  NULL AS pd_pv_id,\n" +
    "  pd.pick_point AS pd_pick_point,\n" +
    "  pd.pick_sort AS pd_pick_sort,\n" +
    "  pd.lc_code AS pd_lc_code,\n" +
    "  pd.receiving_id AS pd_receiving_id,\n" +
    "  pd.receiving_code AS pd_receiving_code,\n" +
    "  pd.po_code AS pd_po_code,\n" +
    "  pd.pd_fifo_time AS pd_fifo_time,\n" +
    "  pd.pd_add_time AS pd_add_time,\n" +
    "  pd.pd_update_time AS pd_update_time,\n" +
    "  pd.is_flow_volume AS pd_is_flow_volume,\n" +
    "  NVL(v_is_flow_volume.vl_name, pd.is_flow_volume) AS pd_is_flow_volume_val,\n" +
    "  NULL AS pd_sorting_sort,\n" +
    "  NULL AS pd_c_code,\n" +
    "  NULL AS pd_is_abnormal,\n" +
    "  NULL AS pd_is_abnormal_val,\n" +
    "  NULL AS pd_pick_quantity,\n" +
    "  NULL AS pd_pick_status,\n" +
    "  NULL AS pd_pick_status_val,\n" +
    "  NULL AS pd_basket_no,\n" +
    "  pd.wa_code AS pd_wa_code, -- \n" +
    "  NULL AS pd_is_supplement,\n" +
    "  NULL AS pd_is_supplement_val,\n" +
    "  NULL AS pd_sync_status,\n" +
    "  NULL AS pd_sync_status_val,\n" +
    "  NULL AS pd_sync_error_message,\n" +
    "  NULL AS pd_abnormal_type,\n" +
    "  NULL AS pd_abnormal_type_val,\n" +
    "  NULL AS pd_order_exception_sub_type,\n" +
    "  NULL AS pd_order_exception_sub_type_val,\n" +
    "  date_format(pd.pd_add_time, 'yyyyMM') month\n" +
    "FROM ods.gc_wms_picking_detail pd\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "WHERE vl_type = 'is_flow_volume' AND vl_datasource_table = 'gc_owms_picking_detail_talbe'\n" +
    ") AS v_is_flow_volume ON pd.is_flow_volume = v_is_flow_volume.vl_value "

  val zy_wms = "SELECT \n" +
    "  pd.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  pd.datasource_num_id AS datasource_num_id,\n" +
    "  pd.data_flag AS data_flag,\n" +
    "  pd.pd_id AS integration_id,\n" +
    "  pd.created_on_dt AS created_on_dt,\n" +
    "  pd.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "\n" +
    "  concat(pd.datasource_num_id, pd.pd_id) AS product_key,\n" +
    "\n" +
    "  pd.pd_id AS pd_id,\n" +
    "  pd.picking_id AS pd_picking_id,\n" +
    "  pd.picking_code AS pd_picking_code,\n" +
    "  pd.order_id AS pd_order_id,\n" +
    "  pd.order_code AS pd_order_code,\n" +
    "  pd.pd_status AS pd_status,\n" +
    "  pd.op_id AS pd_op_id,\n" +
    "  pd.product_id AS pd_product_id,\n" +
    "  pd.product_barcode AS pd_product_barcode,\n" +
    "  pd.pd_quantity AS pd_quantity,\n" +
    "  pd.scan_quantity AS pd_scan_quantity,\n" +
    "  pd.ibo_id AS pd_ibo_id,\n" +
    "  pd.ib_id AS pd_ib_id,\n" +
    "  NULL AS pd_pv_id,\n" +
    "  pd.pick_point AS pd_pick_point,\n" +
    "  pd.pick_sort AS pd_pick_sort,\n" +
    "  pd.lc_code AS pd_lc_code,\n" +
    "  pd.receiving_id AS pd_receiving_id,\n" +
    "  pd.receiving_code AS pd_receiving_code,\n" +
    "  pd.po_code AS pd_po_code,\n" +
    "  pd.pd_fifo_time AS pd_fifo_time,\n" +
    "  pd.pd_add_time AS pd_add_time,\n" +
    "  pd.pd_update_time AS pd_update_time,\n" +
    "  NULL AS pd_is_flow_volume,\n" +
    "  NULL AS pd_is_flow_volume_val,\n" +
    "  NULL AS pd_sorting_sort,\n" +
    "  NULL AS pd_c_code,\n" +
    "  NULL AS pd_is_abnormal,\n" +
    "  NULL AS pd_is_abnormal_val,\n" +
    "  NULL AS pd_pick_quantity,\n" +
    "  NULL AS pd_pick_status,\n" +
    "  NULL AS pd_pick_status_val,\n" +
    "  NULL AS pd_basket_no,\n" +
    "  pd.wa_code AS pd_wa_code, \n" +
    "  NULL AS pd_is_supplement,\n" +
    "  NULL AS pd_is_supplement_val,\n" +
    "  NULL AS pd_sync_status,\n" +
    "  NULL AS pd_sync_status_val,\n" +
    "  NULL AS pd_sync_error_message,\n" +
    "  NULL AS pd_abnormal_type,\n" +
    "  NULL AS pd_abnormal_type_val,\n" +
    "  NULL AS pd_order_exception_sub_type,\n" +
    "  NULL AS pd_order_exception_sub_type_val,\n" +
    "  date_format(pd.pd_add_time, 'yyyyMM') month\n" +
    "FROM ods.zy_wms_picking_detail pd"

  def main(args: Array[String]): Unit = {

    val sqls: Array[String] = getGcOwmsSql.union(getZyOwmsSql).union(Array(gc_wms, zy_wms)).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))

    Dw_fact_common.getRunCode_hive_full_Into(task, dwTable, sqls,
      Array(
        SystemCodeUtil.GC_WMS,
        SystemCodeUtil.ZY_WMS,
        SystemCodeUtil.GC_OWMS_USEA,
        SystemCodeUtil.GC_OWMS_USWE,
        SystemCodeUtil.GC_OWMS_AU,
        SystemCodeUtil.GC_OWMS_CZ,
        SystemCodeUtil.GC_OWMS_DE,
        SystemCodeUtil.GC_OWMS_ES,
        SystemCodeUtil.GC_OWMS_FRVI,
        SystemCodeUtil.GC_OWMS_IT,
        SystemCodeUtil.GC_OWMS_JP,
        SystemCodeUtil.GC_OWMS_UK,
        SystemCodeUtil.GC_OWMS_UKOB,
        SystemCodeUtil.GC_OWMS_USNB,
        SystemCodeUtil.GC_OWMS_USOT,
        SystemCodeUtil.GC_OWMS_USSC,
        SystemCodeUtil.ZY_OWMS_AU,
        SystemCodeUtil.ZY_OWMS_CZ,
        SystemCodeUtil.ZY_OWMS_DE,
        SystemCodeUtil.ZY_OWMS_RU,
        SystemCodeUtil.ZY_OWMS_UK,
        SystemCodeUtil.ZY_OWMS_USEA,
        SystemCodeUtil.ZY_OWMS_USWE,
        SystemCodeUtil.ZY_OWMS_USSC
      )
    )

  }

}
