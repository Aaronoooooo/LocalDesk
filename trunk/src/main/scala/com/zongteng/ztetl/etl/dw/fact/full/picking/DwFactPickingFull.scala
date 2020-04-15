package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}

object DwFactPickingFull {

  // 任务名称
  val task = "Spark_Etl_Dw_Fact_Advance_Picking_Detail"

  // dw层事实表表名
  val tableName = "fact_picking"


  def getGcOwmsSql() = {

    val tables = Array(
      "gc_owms_usea_picking",
      "gc_owms_uswe_picking",
      "gc_owms_au_picking",
      "gc_owms_cz_picking",
      "gc_owms_de_picking",
      "gc_owms_es_picking",
      "gc_owms_frvi_picking",
      "gc_owms_it_picking",
      "gc_owms_jp_picking",
      "gc_owms_uk_picking",
      "gc_owms_ukob_picking",
      "gc_owms_usnb_picking",
      "gc_owms_usot_picking",
      "gc_owms_ussc_picking")

    val sql = "SELECT\n" +
      "  pck.row_wid AS row_wid,\n" +
      "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
      "  current_timestamp ( ) AS w_insert_dt,\n" +
      "  current_timestamp ( ) AS w_update_dt,\n" +
      "  pck.datasource_num_id AS datasource_num_id,\n" +
      "  pck.data_flag AS data_flag,\n" +
      "  pck.picking_id AS integration_id,\n" +
      "  pck.created_on_dt AS created_on_dt,\n" +
      "  pck.changed_on_dt AS changed_on_dt,\n" +
      "  NULL AS timezone,\n" +
      "  NULL AS exchange_rate,\n" +
      "\n" +
      "  CONCAT(pck.datasource_num_id, pck.warehouse_id) AS warehouse_key,\n" +
      "  \n" +
      "  pck.picking_id AS picking_id,\n" +
      "  pck.warehouse_id AS picking_warehouse_id,\n" +
      "  pck.picking_code AS picking_code,\n" +
      "  pck.print_quantity AS picking_print_quantity,\n" +
      "  pck.picker_id AS picking_picker_id,\n" +
      "  pck.creater_id AS picking_creater_id,\n" +
      "  pck.picking_order_cnt AS picking_order_cnt,\n" +
      "  pck.picking_lc_cnt AS picking_lc_cnt, \n" +
      "  pck.picking_item_cnt AS picking_item_cnt,\n" +
      "  pck.osot_code_str AS picking_osot_code_str,\n" +
      "  pck.picking_mode AS picking_mode,\n" +
      "  NVL(pm.pm_name, pck.picking_mode) AS picking_mode_val,\n" +
      "\n" +
      "  pck.picking_status AS picking_status,\n" +
      "  NVL(v_picking_status.vl_name, pck.picking_status) AS picking_status_val,\n" +
      "  pck.picking_sync_status AS picking_sync_status,\n" +
      "  pck.picking_sync_time AS picking_sync_time,\n" +
      "  pck.is_assign AS picking_is_assign,\n" +
      "  NVL(v_is_assign.vl_name, pck.is_assign) AS picking_is_assign_val,\n" +
      "\n" +
      "  pck.picking_pack_check AS picking_pack_check,\n" +
      "  NVL(v_picking_pack_check.vl_name, pck.picking_pack_check) AS picking_pack_check_val,\n" +
      "  pck.picking_type AS picking_type,\n" +
      "  NVL(v_picking_type.vl_name, pck.picking_type) AS picking_type_val,\n" +
      "  pck.picking_add_time AS picking_add_time,\n" +
      "  pck.picking_update_time AS picking_update_time,\n" +
      "  pck.is_more_box AS picking_is_more_box,\n" +
      "  NVL(v_is_more_box.vl_name, pck.is_more_box) AS is_more_box_val,\n" +
      "  pck.is_print AS picking_is_print,\n" +
      "  pck.picking_sort AS picking_sort,\n" +
      "  NVL(v_picking_sort.vl_name, pck.picking_sort) AS picking_sort_val,\n" +
      "  pck.task_id AS picking_task_id,\n" +
      "  pck.wellen_code AS picking_wellen_code,\n" +
      "  pck.sorting_mode AS picking_orting_mode,\n" +
      "  NVL(v_sorting_mode.vl_name, pck.sorting_mode) AS sorting_mode_val,\n" +
      "  pck.sorting_status AS picking_sorting_status,\n" +
      "  NVL(v_sorting_status.vl_name, pck.sorting_status) AS sorting_status_val,\n" +
      "  pck.sorting_time AS picking_sorting_time,\n" +
      "  pck.sorting_user_id AS picking_sorting_user_id,\n" +
      "  pck.sorting_count AS picking_sorting_count,\n" +
      "  pck.is_run_picking AS picking_is_run_picking,\n" +
      "  NVL(v_is_run_picking.vl_name, pck.is_run_picking) AS is_run_picking_val,\n" +
      "  pck.is_cross_warehouse AS picking_is_cross_warehouse,\n" +
      "  NVL(v_is_cross_warehouse.vl_name, pck.is_cross_warehouse) AS is_cross_warehouse_val,\n" +
      "  pck.operate_status AS picking_operate_status,\n" +
      "  NVL(v_operate_status.vl_name, pck.operate_status) AS operate_status_val,\n" +
      "  pck.is_supplement AS picking_is_supplement,\n" +
      "  NVL(v_is_supplement.vl_name, pck.is_supplement) AS is_supplement_val,\n" +
      "  pck.ct_id AS picking_ct_id,\n" +
      "  pck.bind_container_time AS picking_bind_container_time,\n" +
      "  pck.new_task_id AS picking_new_task_id,\n" +
      "  pck.lc_level_type AS picking_lc_level_type,\n" +
      "  NVL(v_lc_level_type.vl_name, pck.lc_level_type) AS lc_level_type_val,\n" +
      "  pck.is_fba AS picking_is_fba,\n" +
      "  NVL(v_is_fba.vl_name, pck.is_fba) AS is_fba_val,\n" +
      "  pck.fba_pick_type AS picking_fba_pick_type,\n" +
      "  NVL(v_fba_pick_type.vl_name, pck.fba_pick_type) AS fba_pick_type_val,\n" +
      "\n" +
      "  NULL AS picking_refrence_no,\n" +
      "  NULL AS picking_is_confirm,\n" +
      "  NULL AS picking_min,\n" +
      "  NULL AS picking_eta_min,\n" +
      "  NULL AS picking_begin_time,\n" +
      "  NULL AS picking_end_time,\n" +
      "\n" +
      "  date_format(pck.picking_add_time, 'yyyyMM') month\n" +
      "FROM ods.picking_table pck\n" +
      "LEFT JOIN ods.gc_owms_usea_picking_mode pm ON pck.picking_mode = pm.pm_id\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'picking_status' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_picking_status ON pck.picking_status = v_picking_status.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'is_assign' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_is_assign ON pck.is_assign = v_is_assign.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'picking_pack_check' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_picking_pack_check ON pck.picking_pack_check = v_picking_pack_check.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'picking_type' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_picking_type ON pck.picking_type = v_picking_type.vl_value\n" +
      "  \n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'is_more_box' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_is_more_box ON pck.is_more_box = v_is_more_box.vl_value \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'picking_sort' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_picking_sort ON pck.picking_sort = v_picking_sort.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'sorting_mode' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_sorting_mode ON pck.sorting_mode = v_sorting_mode.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'sorting_status' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_sorting_status ON pck.sorting_status = v_sorting_status.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'is_run_picking' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_is_run_picking ON pck.is_run_picking = v_is_run_picking.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'is_cross_warehouse' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_is_cross_warehouse ON pck.is_cross_warehouse = v_is_cross_warehouse.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'operate_status' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_operate_status ON pck.operate_status = v_operate_status.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'is_supplement' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_is_supplement ON pck.is_supplement = v_is_supplement.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'lc_level_type' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_lc_level_type ON pck.lc_level_type = v_lc_level_type.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'is_fba' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_is_fba ON pck.is_fba = v_is_fba.vl_value\n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
      "WHERE vl_type = 'fba_pick_type' AND vl_datasource_table = 'picking_table'\n" +
      ") AS v_fba_pick_type ON pck.fba_pick_type = v_fba_pick_type.vl_value"

    tables.map(sql.replace("picking_table", _))
  }

  def getZyOwmsSql() = {

    val sql = "SELECT\n" +
      " pk.row_wid AS row_wid,\n" +
      " date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
      " current_timestamp ( ) AS w_insert_dt,\n" +
      " current_timestamp ( ) AS w_update_dt,\n" +
      " pk.datasource_num_id AS datasource_num_id,\n" +
      " pk.data_flag AS data_flag,\n" +
      " pk.picking_id AS integration_id,\n" +
      " pk.created_on_dt AS created_on_dt,\n" +
      " pk.changed_on_dt AS changed_on_dt,\n" +
      " NULL AS timezone,\n" +
      " NULL AS exchange_rate,\n" +
      "\n" +
      " CONCAT(pk.datasource_num_id, pk.warehouse_id) AS warehouse_key,\n" +
      "\n" +
      " pk.picking_id AS picking_id,\n" +
      " pk.warehouse_id AS picking_warehouse_id,\n" +
      " pk.picking_code AS picking_code,\n" +
      " pk.print_quantity AS picking_print_quantity,\n" +
      " pk.picker_id AS picking_picker_id,\n" +
      " pk.creater_id AS picking_creater_id,\n" +
      " pk.picking_order_cnt AS picking_order_cnt,\n" +
      " pk.picking_lc_cnt AS picking_lc_cnt,\n" +
      " pk.picking_item_cnt AS picking_item_cnt,\n" +
      " pk.osot_code_str AS picking_osot_code_str,\n" +
      " pk.picking_mode AS picking_mode,\n" +
      " NVL(pm.pm_name, pk.picking_mode) AS picking_mode_val,\n" +
      " pk.picking_status AS picking_status,\n" +
      " NVL(v_picking_status.vl_name, pk.picking_status) AS picking_status_val,\n" +
      " pk.picking_sync_status AS picking_sync_status,\n" +
      " pk.picking_sync_time AS picking_sync_time,\n" +
      " pk.is_assign AS picking_is_assign,\n" +
      " NVL(v_is_assign.vl_name, pk.is_assign) AS picking_is_assign_val,\n" +
      " pk.picking_pack_check AS picking_pack_check,\n" +
      " NVL(v_picking_pack_check.vl_name, pk.picking_pack_check) AS picking_pack_check_val,\n" +
      " pk.picking_type AS picking_type,\n" +
      " NVL(v_picking_type.vl_name, pk.picking_type) AS picking_type_val,\n" +
      " pk.picking_add_time AS picking_add_time,\n" +
      " pk.picking_update_time AS picking_update_time,\n" +
      " pk.is_more_box AS picking_is_more_box,\n" +
      " NVL(v_is_more_box.vl_name, pk.is_more_box) AS picking_is_more_box,\n" +
      " pk.is_print AS picking_is_print,\n" +
      " pk.picking_sort AS picking_sort,\n" +
      " NVL(v_picking_sort.vl_name, pk.picking_sort) AS picking_sort_val,\n" +
      " pk.task_id AS picking_task_id,\n" +
      " pk.wellen_code AS picking_wellen_code,\n" +
      " NULL AS picking_sorting_mode,\n" +
      " NULL AS picking_sorting_mode_val,\n" +
      " NULL AS picking_sorting_status,\n" +
      " NULL AS picking_sorting_status_val,\n" +
      " NULL AS picking_sorting_time,\n" +
      " NULL AS picking_sorting_user_id,\n" +
      " NULL AS picking_sorting_count,\n" +
      " NULL AS picking_is_run_picking,\n" +
      " NULL AS picking_is_run_picking_val,\n" +
      " NULL AS picking_is_cross_warehouse,\n" +
      " NULL AS picking_is_cross_warehouse_val,\n" +
      " NULL AS picking_operate_status,\n" +
      " NULL AS picking_operate_status_val,\n" +
      " NULL AS picking_is_supplement,\n" +
      " NULL AS picking_is_supplement_val,\n" +
      " NULL AS picking_ct_id,\n" +
      " NULL AS picking_bind_container_time,\n" +
      " NULL AS picking_new_task_id,\n" +
      " NULL AS picking_lc_level_type,\n" +
      " NULL AS picking_lc_level_type_val,\n" +
      " NULL AS picking_is_fba,\n" +
      " NULL AS picking_is_fba_val,\n" +
      " NULL AS picking_fba_pick_type,\n" +
      " NULL AS picking_fba_pick_type_val,\n" +
      " NULL AS picking_refrence_no,\n" +
      " NULL AS picking_is_confirm,\n" +
      " NULL AS picking_min,\n" +
      " NULL AS picking_eta_min,\n" +
      " NULL AS picking_begin_time,\n" +
      " NULL AS picking_end_time,\n" +
      "\n" +
      " date_format(pk.picking_add_time, 'yyyyMM') month\n" +
      "FROM ods.zy_owms_picking_table pk\n" +
      "LEFT JOIN ods.zy_owms_au_picking_mode as pm on pk.picking_mode = pm.pm_id \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "  WHERE vl_type = 'picking_status'\n" +
      "  AND vl_datasource_table = 'zy_owms_picking_table'\n" +
      "  ) AS v_picking_status ON pk.picking_status = v_picking_status.vl_value   \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "  WHERE vl_type = 'is_assign'\n" +
      "  AND vl_datasource_table = 'zy_owms_picking_table'\n" +
      "  ) AS v_is_assign ON pk.is_assign = v_is_assign.vl_value   \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "  WHERE vl_type = 'picking_pack_check'\n" +
      "  AND vl_datasource_table = 'zy_owms_picking_table'\n" +
      "  ) AS v_picking_pack_check ON pk.picking_pack_check = v_picking_pack_check.vl_value   \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "  WHERE vl_type = 'picking_type'\n" +
      "  AND vl_datasource_table = 'zy_owms_picking_table'\n" +
      "  ) AS v_picking_type ON pk.picking_type = v_picking_type.vl_value   \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "  WHERE vl_type = 'is_more_box'\n" +
      "  AND vl_datasource_table = 'zy_owms_picking_table'\n" +
      "  ) AS v_is_more_box ON pk.is_more_box = v_is_more_box.vl_value   \n" +
      "\n" +
      "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
      "  WHERE vl_type = 'picking_sort'\n" +
      "  AND vl_datasource_table = 'zy_owms_picking_table'\n" +
      "  ) AS v_picking_sort ON pk.picking_sort = v_picking_sort.vl_value"

    val tables = Array(
      "zy_owms_au_picking",
      "zy_owms_cz_picking",
      "zy_owms_de_picking",
      "zy_owms_ru_picking",
      "zy_owms_uk_picking",
      "zy_owms_usea_picking",
      "zy_owms_uswe_picking",
      "zy_owms_ussc_picking"
    )

    tables.map(sql.replace("zy_owms_picking_table", _))
  }

  val gc_wms =
    "SELECT\n" +
    "  pk.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  pk.datasource_num_id AS datasource_num_id,\n" +
    "  pk.data_flag AS data_flag,\n" +
    "  pk.picking_id AS integration_id,\n" +
    "  pk.created_on_dt AS created_on_dt,\n" +
    "  pk.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "\n" +
    "  CONCAT(pk.datasource_num_id, pk.warehouse_id) AS warehouse_key,\n" +
    "\n" +
    "  pk.picking_id AS picking_id,\n" +
    "  pk.warehouse_id AS picking_warehouse_id,\n" +
    "  pk.picking_code AS picking_code,\n" +
    "  pk.print_quantity AS picking_print_quantity,\n" +
    "  pk.picker_id AS picking_picker_id,\n" +
    "  pk.creater_id AS picking_creater_id,\n" +
    "  pk.picking_order_cnt AS picking_order_cnt,\n" +
    "  pk.picking_lc_cnt AS picking_lc_cnt,\n" +
    "  pk.picking_item_cnt AS picking_item_cnt,\n" +
    "  NULL AS picking_osot_code_str,\n" +
    "  pk.picking_mode AS picking_mode,\n" +
    "  NVL(pm.pm_name, pk.picking_mode) AS picking_mode_val,\n" +
    "  pk.picking_status AS picking_status,\n" +
    "  NVL(v_picking_status.vl_name, pk.picking_status) AS picking_status_val,\n" +
    "  NULL AS picking_sync_status,\n" +
    "  NULL AS picking_sync_time,\n" +
    "  pk.is_assign AS picking_is_assign,\n" +
    "  NVL(v_is_assign.vl_name, pk.is_assign) AS picking_is_assign_val,\n" +
    "  NULL AS picking_pack_check,\n" +
    "  NULL AS picking_pack_check_val,\n" +
    "  pk.picking_type AS picking_type,\n" +
    "  NVL(v_picking_type.vl_name, pk.picking_type) AS picking_type_val,\n" +
    "\n" +
    "  pk.picking_add_time AS picking_add_time,\n" +
    "\n" +
    "  pk.picking_update_time AS picking_update_time,\n" +
    "\n" +
    "  pk.is_more_box AS picking_is_more_box,\n" +
    "  NVL(v_is_more_box.vl_name, pk.is_more_box) AS is_more_box_val,\n" +
    "\n" +
    "  is_print AS picking_is_print,\n" +
    "\n" +
    "  NULL AS picking_sort,\n" +
    "  NULL AS picking_sort_val,\n" +
    "\n" +
    "  NULL AS picking_task_id,\n" +
    "  NULL AS picking_wellen_code,\n" +
    "  NULL AS picking_orting_mode,\n" +
    "  NULL AS sorting_mode_val,\n" +
    "  NULL AS picking_sorting_status,\n" +
    "  NULL AS sorting_status_val,\n" +
    "  NULL AS picking_sorting_time,\n" +
    "\n" +
    "  NULL AS picking_sorting_user_id,\n" +
    "  NULL AS picking_sorting_count,\n" +
    "  NULL AS picking_is_run_picking,\n" +
    "  NULL AS is_run_picking_val,\n" +
    "  NULL AS picking_is_cross_warehouse,\n" +
    "  NULL AS is_cross_warehouse_val,\n" +
    "  NULL AS picking_operate_status,\n" +
    "  NULL AS operate_status_val,\n" +
    "  NULL AS picking_is_supplement,\n" +
    "  NULL AS is_supplement_val,\n" +
    "  NULL AS picking_ct_id,\n" +
    "  NULL AS picking_bind_container_time,\n" +
    "  NULL AS picking_new_task_id,\n" +
    "  NULL AS picking_lc_level_type,\n" +
    "  NULL AS lc_level_type_val,\n" +
    "  NULL AS picking_is_fba,\n" +
    "  NULL AS is_fba_val,\n" +
    "  NULL AS picking_fba_pick_type,\n" +
    "  NULL AS fba_pick_type_val,\n" +
    "\n" +
    "  pk.refrence_no AS picking_refrence_no,\n" +
    "  pk.is_confirm AS picking_is_confirm,\n" +
    "  pk.picking_min AS picking_min,\n" +
    "  pk.picking_eta_min AS picking_eta_min,\n" +
    "  pk.picking_begin_time AS picking_begin_time,\n" +
    "  pk.picking_end_time AS picking_end_time,\n" +
    "  date_format(pk.picking_add_time, 'yyyyMM') month\n" +
    "FROM ods.gc_wms_picking pk\n" +
    "LEFT JOIN ods.gc_wms_picking_mode pm ON pk.picking_mode = pm.pm_id\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'picking_status' AND vl_datasource_table = 'gc_wms_picking'\n" +
    ") AS v_picking_status ON pk.picking_status = v_picking_status.vl_value   \n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'is_assign' AND vl_datasource_table = 'gc_wms_picking'\n" +
    ") AS v_is_assign ON pk.is_assign = v_is_assign.vl_value   \n" +
    "\n" +
    "LEFT JOIN ( SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'picking_type' AND vl_datasource_table = 'gc_wms_picking'\n" +
    ") AS v_picking_type ON pk.picking_type = v_picking_type.vl_value   \n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'is_more_box' AND vl_datasource_table = 'gc_wms_picking'\n" +
    ") AS v_is_more_box ON pk.is_more_box = v_is_more_box.vl_value   "

  val zy_wms = "SELECT\n" +
    "  pk.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  pk.datasource_num_id AS datasource_num_id,\n" +
    "  pk.data_flag AS data_flag,\n" +
    "  pk.picking_id AS integration_id,\n" +
    "  pk.created_on_dt AS created_on_dt,\n" +
    "  pk.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "  CONCAT(pk.datasource_num_id, pk.warehouse_id) AS warehouse_key,\n" +
    "  pk.picking_id AS picking_id,\n" +
    "  pk.warehouse_id AS picking_warehouse_id,\n" +
    "  pk.picking_code AS picking_code,\n" +
    "  pk.print_quantity AS picking_print_quantity,\n" +
    "  pk.picker_id AS picking_picker_id,\n" +
    "  pk.creater_id AS picking_creater_id,\n" +
    "  pk.picking_order_cnt AS picking_order_cnt,\n" +
    "  pk.picking_lc_cnt AS picking_lc_cnt,\n" +
    "  pk.picking_item_cnt AS picking_item_cnt,\n" +
    "  NULL AS picking_osot_code_str,\n" +
    "  pk.picking_mode AS picking_mode,\n" +
    "  NVL(pm.pm_name, pk.picking_mode) AS picking_mode_val,\n" +
    "  pk.picking_status AS picking_status,\n" +
    "  NVL(v_picking_status.vl_name, pk.picking_status) AS picking_status_val,\n" +
    "\n" +
    "  NULL AS picking_sync_status,\n" +
    "  NULL AS picking_sync_time,\n" +
    "  pk.is_assign AS picking_is_assign,\n" +
    "  NVL(v_is_assign.vl_name, pk.is_assign) AS picking_is_assign_val,\n" +
    "\n" +
    "  NULL AS picking_pack_check,\n" +
    "  NULL AS picking_pack_check_val,\n" +
    "  pk.picking_type AS picking_type,\n" +
    "  NVL(v_picking_type.vl_name, pk.picking_type) AS picking_type_val,\n" +
    "\n" +
    "  pk.picking_add_time AS picking_add_time,\n" +
    "\n" +
    "  pk.picking_update_time AS picking_update_time,\n" +
    "\n" +
    "  pk.is_more_box AS picking_is_more_box,\n" +
    "  NVL(v_is_more_box.vl_name, pk.is_more_box) AS is_more_box_val,\n" +
    "\n" +
    "  NULL AS picking_is_print,\n" +
    "\n" +
    "  NULL AS picking_sort,\n" +
    "  NULL AS picking_sort_val,\n" +
    "\n" +
    "  NULL AS picking_task_id,\n" +
    "  NULL AS picking_wellen_code,\n" +
    "  NULL AS picking_orting_mode,\n" +
    "  NULL AS sorting_mode_val,\n" +
    "  NULL AS picking_sorting_status,\n" +
    "  NULL AS sorting_status_val,\n" +
    "  NULL AS picking_sorting_time,\n" +
    "\n" +
    "  NULL AS picking_sorting_user_id,\n" +
    "  NULL AS picking_sorting_count,\n" +
    "  NULL AS picking_is_run_picking,\n" +
    "  NULL AS is_run_picking_val,\n" +
    "  NULL AS picking_is_cross_warehouse,\n" +
    "  NULL AS is_cross_warehouse_val,\n" +
    "  NULL AS picking_operate_status,\n" +
    "  NULL AS operate_status_val,\n" +
    "  NULL AS picking_is_supplement,\n" +
    "  NULL AS is_supplement_val,\n" +
    "  NULL AS picking_ct_id,\n" +
    "  NULL AS picking_bind_container_time,\n" +
    "  NULL AS picking_new_task_id,\n" +
    "  NULL AS picking_lc_level_type,\n" +
    "  NULL AS lc_level_type_val,\n" +
    "  NULL AS picking_is_fba,\n" +
    "  NULL AS is_fba_val,\n" +
    "  NULL AS picking_fba_pick_type,\n" +
    "  NULL AS fba_pick_type_val,\n" +
    "\n" +
    "  pk.refrence_no AS picking_refrence_no,\n" +
    "  pk.is_confirm AS picking_is_confirm,\n" +
    "  pk.picking_min AS picking_min,\n" +
    "  pk.picking_eta_min AS picking_eta_min,\n" +
    "  pk.picking_begin_time AS picking_begin_time,\n" +
    "  pk.picking_end_time AS picking_end_time,\n" +
    "  date_format(pk.picking_add_time, 'yyyyMM') month\n" +
    "FROM ods.zy_wms_picking pk\n" +
    "LEFT JOIN ods.zy_wms_picking_mode pm ON pk.picking_mode = pm.pm_id\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'picking_status' AND vl_datasource_table = 'zy_wms_picking'\n" +
    ") AS v_picking_status ON pk.picking_status = v_picking_status.vl_value   \n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'is_assign' AND vl_datasource_table = 'zy_wms_picking'\n" +
    ") AS v_is_assign ON pk.is_assign = v_is_assign.vl_value   \n" +
    "\n" +
    "LEFT JOIN ( SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'picking_type' AND vl_datasource_table = 'zy_wms_picking'\n" +
    ") AS v_picking_type ON pk.picking_type = v_picking_type.vl_value   \n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list\n" +
    "  WHERE vl_type = 'is_more_box' AND vl_datasource_table = 'zy_wms_picking'\n" +
    ") AS v_is_more_box ON pk.is_more_box = v_is_more_box.vl_value "

  def main(args: Array[String]): Unit = {

    val sqls: Array[String] = getGcOwmsSql.union(getZyOwmsSql).union(Array(gc_wms, zy_wms)).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))

    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqls,
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
     ))
  }





}
