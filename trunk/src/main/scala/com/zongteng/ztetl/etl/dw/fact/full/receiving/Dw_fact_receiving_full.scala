package com.zongteng.ztetl.etl.dw.fact.full.receiving

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_receiving_full {

  // 任务名称(一般同类名)
  private val task = "Dw_fact_receiving_full"

  // dw层类名
  private val tableName = "fact_receiving"

  // 获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  private val builder = new StringBuilder

  // 要执行的sql语句
  private val gc_oms = "SELECT\n" +
    "  rc.row_wid AS row_wid,\n" +
    "  cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMMdd' ) AS string ) AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  rc.datasource_num_id AS datasource_num_id,\n" +
    "  rc.data_flag AS data_flag,\n" +
    "  rc.receiving_id AS integration_id,\n" +
    "  rc.created_on_dt AS created_on_dt,\n" +
    "  rc.changed_on_dt AS changed_on_dt,\n" +
    "  0 AS timezone,\n" +
    "  0.00 AS exchange_rate, \n" +
    "\n" +
    "  concat(rc.datasource_num_id, sm.sm_id)  AS sm_key,\n" +
    "  concat(rc.datasource_num_id, rc.transit_warehouse_id) AS transit_warehouse_key,\n" +
    "  concat(rc.datasource_num_id, rc.warehouse_id) AS warehouse_key,\n" +
    "  concat(rc.datasource_num_id, rc.warehouse_id) AS to_warehouse_key,\n" +
    "  concat(rc.datasource_num_id, rc.customer_id) AS customer_key,\n" +
    "\n" +
    "  rc.receiving_id AS receiving_id,\n" +
    "  sm.sm_id AS receiving_sm_id,\n" +
    "  rc.sm_code AS receiving_sm_code,\n" +
    "  rc.receiving_code AS receiving_code,\n" +
    "  rc.tracking_number AS receiving_tracking_number,\n" +
    "  rc.shipping_method AS receiving_shipping_method,\n" +
    "  rc.income_type AS receiving_income_type,\n" +
    "  v_income_type.vl_bi_name AS receiving_income_type_val,\n" +
    "  rc.po_code AS receiving_po_code,\n" +
    "  rc.reference_no AS receiving_reference_no,\n" +
    "  rc.transit_warehouse_id  AS receiving_transit_warehouse_id,\n" +
    "  rc.transit_warehouse_code AS receiving_transit_warehouse_code,\n" +
    "  rc.warehouse_id AS receiving_warehouse_id,\n" +
    "  rc.warehouse_code AS receiving_warehouse_code,\n" +
    "  null AS receiving_to_warehouse_id,\n" +
    "  null AS receiving_to_warehouse_code,\n" +
    "  rc.supplier_id AS receiving_supplier_id,\n" +
    "  rc.receiving_update_user AS receiving_update_user,\n" +
    "  rc.receiving_add_user AS receiving_add_user,\n" +
    "  rc.customer_id AS receiving_customer_id,\n" +
    "  rc.customer_code AS receiving_customer_code,\n" +
    "  rc.receiving_type AS receiving_type,\n" +
    "  nvl(v_receiving_type.vl_bi_name, rc.receiving_type) AS receiving_type_val,\n" +
    "  rc.receiving_status  AS receiving_status,\n" +
    "  nvl(v_receiving_status.vl_bi_name, rc.receiving_status) AS receiving_status_val,\n" +
    "  rc.receiving_source_type AS receiving_source_type,\n" +
    "  rc.contacter AS receiving_contacter,\n" +
    "  rc.contact_phone AS receiving_contact_phone,\n" +
    "  rc.receiving_description AS receiving_description,\n" +
    "  null AS receiving_batch,\n" +
    "  rc.receiving_add_time AS receiving_add_time,\n" +
    "  rc.receiving_update_time AS receiving_update_time,\n" +
    "  null AS receiving_total_packages,\n" +
    "  rc.expected_date AS receiving_expected_date,\n" +
    "  rc.receiving_transfer_status AS receiving_transfer_status,\n" +
    "  nvl(v_receiving_transfer_status.vl_bi_name, rc.receiving_transfer_status) AS receiving_transfer_status_val,\n" +
    "  rc.receiving_exception AS receiving_exception,\n" +
    "  rc.receiving_exception_handle AS receiving_exception_handle,\n" +
    "  rc.receiving_abnormal_confirm AS receiving_abnormal_confirm,\n" +
    "  rc.tax_type AS receiving_tax_type,\n" +
    "  nvl(v_tax_type.vl_bi_name, rc.tax_type) AS receiving_tax_type_val,\n" +
    "  rc.insurance_type AS receiving_insurance_type,\n" +
    "  rc.customer_type AS receiving_customer_type,\n" +
    "  nvl(v_customer_type.vl_bi_name, rc.customer_type) AS receiving_customer_type_val,\n" +
    "  rc.wms_qty_confirm AS receiving_wms_qty_confirm,\n" +
    "  null AS receiving_transit_receiving_code,\n" +
    "  null AS receiving_sync_owms_status,\n" +
    "  null AS receiving_sync_owms_status_val,\n" +
    "  null AS receiving_sync_owms_sign,\n" +
    "  null AS receiving_sync_owms_sign_val,\n" +
    "  null AS receiving_sync_owms_time,\n" +
    "  rc.check_status AS receiving_check_status,\n" +
    "  rc.is_fba AS receiving_is_fba,\n" +
    "  rc.create_type AS receiving_create_type,\n" +
    "  nvl(v_create_type.vl_bi_name, rc.create_type) AS receiving_create_type_val,\n" +
    "  null AS receiving_is_return,\n" +
    "  rc.receiving_shipping_type AS receiving_shipping_type,\n" +
    "  nvl(v_receiving_shipping_type.vl_bi_name, rc.receiving_shipping_type) AS receiving_shipping_type_val,\n" +
    "  rc.received_packages AS received_packages,\n" +
    "  rc.container_number AS receiving_container_number,\n" +
    "  rc.first_sign_time AS receiving_first_sign_time,\n" +
    "  rc.last_sign_time AS receiving_last_sign_time,\n" +
    "  rc.rv_sign_batch AS receiving_rv_sign_batch,\n" +
    "  rc.rv_sign_status AS receiving_rv_sign_status,\n" +
    "  nvl(v_rv_sign_status.vl_bi_name, rc.rv_sign_status) AS receiving_rv_sign_status_val,\n" +
    "  rc.r_timestamp AS receiving_rv_timestamp,\n" +
    "  rc.volume AS receiving_volume,\n" +
    "  rc.weight AS receiving_weight,\n" +
    "  null AS receiving_is_fee_message,\n" +
    "  null AS receiving_is_fee,\n" +
    "  null AS receiving_is_fee_val,\n" +
    "  rc.is_change_sku AS receiving_is_change_sku,\n" +
    "  nvl(v_is_change_sku.vl_bi_name, rc.is_change_sku) AS receiving_is_change_sku_val,\n" +
    "  rc.is_third AS receiving_is_third,\n" +
    "  nvl(v_is_third.vl_bi_name, rc.is_third) AS receiving_is_third_val,\n" +
    "  rc.take_stock_code AS receiving_take_stock_code,\n" +
    "  null AS receiving_vat_number,\n" +
    "  null AS receiving_exemption_number,\n" +
    "  null AS receiving_eori,\n" +
    "  null AS receiving_auditing_user_id,\n" +
    "  null AS receiving_auditing_user_name,\n" +
    "  null AS receiving_reject_content,\n" +
    "  null AS receiving_auditing_time,\n" +
    "  null AS receiving_vat_status,\n" +
    "  null AS receiving_vat_status_val,\n" +
    "  null AS receiving_c88_path,\n" +
    "  null AS receiving_is_cost_fee,\n" +
    "  null AS receiving_is_cost_fee_val,\n" +
    "  null AS receiving_reference_code,\n" +
    "  rc.box_total AS receiving_box_total,\n" +
    "  rc.sku_total AS receiving_sku_total,\n" +
    "  rc.sku_species AS receiving_sku_species,\n" +
    "  null AS receiving_is_send_email,\n" +
    "  null AS receiving_is_send_email_val,\n" +
    "  null AS receiving_email_result,\n" +
    "  null AS receiving_email_result_val,\n" +
    "  null AS receiving_is_diff,\n" +
    "  null AS receiving_is_diff_val,\n" +
    "  null AS receiving_diff_first_time,\n" +
    "  null AS receiving_diff_sure_time,\n" +
    "  null AS receiving_diff_sure_status,\n" +
    "  null AS receiving_diff_sure_status_val,\n" +
    "  null AS receiving_country_code,\n" +
    "  null AS receiving_state,\n" +
    "  null AS receiving_city,\n" +
    "  null AS receiving_district,\n" +
    "  rc.street AS receiving_street,\n" +
    "  null AS receiving_audited_time,\n" +
    "  null AS receiving_deliveried_time,\n" +
    "  null AS receiving_shelved_time,\n" +
    "  null AS receiving_file_status,\n" +
    "  null AS receiving_file_status_val,\n" +
    "  null AS receiving_wp_code,\n" +
    "  null AS receiving_update_mark,\n" +
    "  rc.transit_type AS receiving_transit_type,\n" +
    "  rc.refercence_form_id AS receiving_refercence_form_id,\n" +
    "  rc.ie_port AS receiving_ie_port,\n" +
    "  rc.form_type AS receiving_form_type,\n" +
    "  rc.traf_name AS receiving_traf_name,\n" +
    "  rc.wrap_type AS receiving_wrap_type,\n" +
    "  rc.pack_no AS receiving_pack_no,\n" +
    "  rc.traf_mode AS receiving_traf_mode,\n" +
    "  rc.trade_mode AS receiving_trade_mode,\n" +
    "  rc.trans_mode AS receiving_trans_mode,\n" +
    "  rc.conta_id AS receiving_conta_id,\n" +
    "  rc.conta_model AS receiving_conta_model,\n" +
    "  rc.conta_wt AS receiving_conta_wt,\n" +
    "  rc.haveconta AS receiving_haveconta,\n" +
    "  rc.eda_date AS receiving_eda_date,\n" +
    "  rc.region_0 AS receiving_region_0,\n" +
    "  rc.region_1 AS receiving_region_1,\n" +
    "  rc.region_2 AS receiving_region_2,\n" +
    "  rc.date_release AS receiving_date_release,\n" +
    "  rc.new_add_sku_species AS receiving_new_add_sku_species,\n" +
    "    date_format(receiving_add_time, 'yyyyMM') AS month\n" +
    "FROM (SELECT * FROM ods.gc_oms_receiving WHERE data_flag != 'DELETE')  rc\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("gc_oms_shipping_method","sm")} on sm.sm_code=rc.sm_code \n" +
    "\n" +
    "-- create_type\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'create_type'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_create_type ON rc.create_type = v_create_type.vl_value\n" +
    "\n" +
    "-- receiving_shipping_type\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'receiving_shipping_type'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_receiving_shipping_type ON rc.receiving_shipping_type = v_receiving_shipping_type.vl_value\n" +
    "\n" +
    "\n" +
    "-- rv_sign_status\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'rv_sign_status'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_rv_sign_status ON rc.rv_sign_status = v_rv_sign_status.vl_value\n" +
    "\n" +
    "-- is_change_sku\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'is_change_sku'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_is_change_sku ON rc.is_change_sku = v_is_change_sku.vl_value\n" +
    "\n" +
    "-- is_third\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'is_third'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_is_third ON rc.is_third = v_is_third.vl_value\n" +
    "\n" +
    "\n" +
    "-- receiving_status\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'receiving_status'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_receiving_status ON rc.receiving_status = v_receiving_status.vl_value\n" +
    "\n" +
    "\n" +
    "-- income_type\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'income_type'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_income_type ON rc.income_type = v_income_type.vl_value\n" +
    "\n" +
    "\n" +
    "-- receiving_transfer_status\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'receiving_transfer_status'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_receiving_transfer_status ON rc.receiving_transfer_status = v_receiving_transfer_status.vl_value\n" +
    "\n" +
    "-- tax_type\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'tax_type'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_tax_type ON rc.tax_type = v_tax_type.vl_value\n" +
    "\n" +
    "-- customer_type\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'customer_type'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_customer_type ON rc.customer_type = v_customer_type.vl_value\n" +
    "\n" +
    "-- receiving_type\n" +
    "LEFT JOIN (\n" +
    "  SELECT distinct * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9001'\n" +
    "  AND vl_type = 'receiving_type'\n" +
    "  AND vl_datasource_table = 'gc_oms_receiving'\n" +
    ") AS v_receiving_type ON rc.receiving_type = v_receiving_type.vl_value"


  private val gc_wms = "SELECT\n" +
    "    rc.row_wid AS row_wid,\n" +
    "    cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMMdd' ) AS string ) AS etl_proc_wid,\n" +
    "    current_timestamp ( ) AS w_insert_dt,\n" +
    "    current_timestamp ( ) AS w_update_dt,\n" +
    "    rc.datasource_num_id AS datasource_num_id,\n" +
    "    rc.data_flag AS data_flag,\n" +
    "    rc.receiving_id AS integration_id,\n" +
    "    rc.created_on_dt AS created_on_dt,\n" +
    "    rc.changed_on_dt AS changed_on_dt,\n" +
    "    0 AS timezone,\n" +
    "    0.00 AS exchange_rate,     \n" +
    "\n" +
    "    null AS sm_key,\n" +
    "    null as transit_warehouse_key,\n" +
    "    concat(rc.datasource_num_id, wa.warehouse_id) as warehouse_key,\n" +
    "    concat(rc.datasource_num_id, wa.warehouse_id) as to_warehouse_key,\n" +
    "    concat(rc.datasource_num_id,cu.customer_id) as customer_key,\n" +
    "\n" +
    "  rc.receiving_id AS receiving_id,\n" +
    "  null AS receiving_sm_id,\n" +
    "  null AS receiving_sm_code,\n" +
    "  rc.receiving_code AS receiving_code,\n" +
    "  rc.tracking_number AS receiving_tracking_number,\n" +
    "  null AS receiving_shipping_method,\n" +
    "  rc.income_type AS receiving_income_type,\n" +
    "  nvl(v_income_type.vl_bi_name, rc.income_type) AS receiving_income_type_val,\n" +
    "  null AS receiving_po_code,\n" +
    "  null AS receiving_reference_no,\n" +
    "  null AS receiving_transit_warehouse_id,\n" +
    "  null AS receiving_transit_warehouse_code,\n" +
    "  wa.warehouse_id AS receiving_warehouse_id,\n" +
    "  rc.warehouse_code AS receiving_warehouse_code,\n" +
    "  null AS receiving_to_warehouse_id,\n" +
    "  null AS receiving_to_warehouse_code,\n" +
    "  null AS receiving_supplier_id,\n" +
    "  null AS receiving_update_user,\n" +
    "  null AS receiving_add_user,\n" +
    "  cu.customer_id AS receiving_customer_id,\n" +
    "  rc.customer_code AS receiving_customer_code,\n" +
    "  rc.receiving_type AS receiving_type,\n" +
    "    nvl(v_receiving_type.vl_bi_name, rc.receiving_type) AS receiving_type_val,\n" +
    "  rc.receiving_status AS receiving_status,\n" +
    "  nvl(v_receiving_status.vl_bi_name, rc.receiving_status) AS receiving_status_val,\n" +
    "  null AS receiving_source_type,\n" +
    "  rc.contacter AS receiving_contacter,\n" +
    "  rc.contact_phone as receiving_contact_phone,\n" +
    "  rc.receiving_description AS receiving_description,\n" +
    "  null AS receiving_batch,\n" +
    "  rc.receiving_add_time AS receiving_add_time,\n" +
    "  null AS receiving_update_time,\n" +
    "  null AS receiving_total_packages,\n" +
    "  rc.expected_date AS receiving_expected_date,\n" +
    "  null AS receiving_transfer_status,\n" +
    "  null AS receiving_transfer_status_val,\n" +
    "  null AS receiving_exception,\n" +
    "  null AS receiving_exception_handle,\n" +
    "  null AS receiving_abnormal_confirm,\n" +
    "  null AS receiving_tax_type,\n" +
    "  null AS receiving_tax_type_val,\n" +
    "  null AS receiving_insurance_type,\n" +
    "  null AS receiving_customer_type,\n" +
    "  null AS receiving_customer_type_val,\n" +
    "  null AS receiving_wms_qty_confirm,\n" +
    "  null AS receiving_transit_receiving_code,\n" +
    "  null AS receiving_sync_owms_status,\n" +
    "  null AS receiving_sync_owms_status_val,\n" +
    "  null AS receiving_sync_owms_sign,\n" +
    "  null AS receiving_sync_owms_sign_val,\n" +
    "  null AS receiving_sync_owms_time,\n" +
    "  null AS receiving_check_status,\n" +
    "  null AS receiving_is_fba,\n" +
    "  rc.create_type AS receiving_create_type,\n" +
    "  nvl(v_create_type.vl_bi_name, rc.create_type) AS receiving_create_type_val,\n" +
    "  null AS receiving_is_return,\n" +
    "  rc.receiving_shipping_type AS receiving_shipping_type,\n" +
    "  nvl(v_receiving_shipping_type.vl_bi_name, rc.receiving_shipping_type) AS receiving_shipping_type_val,\n" +
    "  null AS received_packages,\n" +
    "  rc.container_number AS receiving_container_number,\n" +
    "  rc.first_sign_time AS receiving_first_sign_time,\n" +
    "  rc.last_sign_time AS receiving_last_sign_time,\n" +
    "  rc.rv_sign_batch AS receiving_rv_sign_batch,\n" +
    "  rc.rv_sign_status AS receiving_rv_sign_status,\n" +
    "  nvl(v_rv_sign_status.vl_bi_name, rc.rv_sign_status) AS receiving_rv_sign_status_val,\n" +
    "  rc.r_timestamp AS receiving_rv_timestamp,\n" +
    "  null AS receiving_volume,\n" +
    "  null AS receiving_weight,\n" +
    "  null AS receiving_is_fee_message,\n" +
    "  null AS receiving_is_fee,\n" +
    "  null AS receiving_is_fee_val,\n" +
    "  null AS receiving_is_change_sku,\n" +
    "  null AS receiving_is_change_sku_val,\n" +
    "  null AS receiving_is_third,\n" +
    "  null AS receiving_is_third_val,\n" +
    "  rc.take_stock_code AS receiving_take_stock_code,\n" +
    "  rc.vat_number AS receiving_vat_number,\n" +
    "  rc.exemption_number AS receiving_exemption_number,\n" +
    "  rc.eori AS receiving_eori,\n" +
    "  rc.auditing_user_id AS receiving_auditing_user_id,\n" +
    "  rc.auditing_user_name AS receiving_auditing_user_name,\n" +
    "  rc.reject_content AS receiving_reject_content,\n" +
    "  rc.auditing_time AS receiving_auditing_time,\n" +
    "  rc.vat_status AS receiving_vat_status,\n" +
    "  nvl(v_vat_status.vl_bi_name, rc.vat_status) AS receiving_vat_status_val,\n" +
    "  rc.c88_path AS receiving_c88_path,\n" +
    "  null AS receiving_is_cost_fee,\n" +
    "  null AS receiving_is_cost_fee_val,\n" +
    "  rc.reference_code AS receiving_reference_code,\n" +
    "  rc.box_total AS receiving_box_total,\n" +
    "  rc.sku_total AS receiving_sku_total,\n" +
    "  rc.sku_species AS receiving_sku_species,\n" +
    "  rc.is_send_email AS receiving_is_send_email,\n" +
    "  nvl(v_is_send_email.vl_bi_name, rc.is_send_email) AS receiving_is_send_email_val,\n" +
    "  rc.email_result AS receiving_email_result,\n" +
    "  nvl(v_email_result.vl_bi_name, rc.email_result) AS receiving_email_result_val,\n" +
    "  rc.is_diff AS receiving_is_diff,\n" +
    "  v_is_diff.vl_bi_name AS receiving_is_diff_val,\n" +
    "  rc.diff_first_time AS receiving_diff_first_time,\n" +
    "  rc.diff_sure_time AS receiving_diff_sure_time,\n" +
    "  rc.diff_sure_status AS receiving_diff_sure_status,\n" +
    "  nvl(v_diff_sure_status.vl_bi_name, rc.diff_sure_status) AS receiving_diff_sure_status_val,\n" +
    "  rc.country_code AS receiving_country_code,\n" +
    "  rc.state AS receiving_state,\n" +
    "  rc.city AS receiving_city,\n" +
    "  rc.district AS receiving_district,\n" +
    "  rc.street AS receiving_street,\n" +
    "  rc.receiving_audited_time AS receiving_audited_time,\n" +
    "  rc.deliveried_time AS receiving_deliveried_time,\n" +
    "  rc.shelved_time AS receiving_shelved_time,\n" +
    "  rc.file_status AS receiving_file_status,\n" +
    "  nvl(v_file_status.vl_bi_name, rc.file_status) AS receiving_file_status_val,\n" +
    "  rc.wp_code AS receiving_wp_code,\n" +
    "  rc.update_mark AS receiving_update_mark,\n" +
    "  null AS receiving_transit_type, \n" +
    "    null AS receiving_refercence_form_id,\n" +
    "    null AS receiving_ie_port,\n" +
    "    null AS receiving_form_type,\n" +
    "    null AS receiving_traf_name,\n" +
    "    null AS receiving_wrap_type,\n" +
    "    null AS receiving_pack_no,\n" +
    "    null AS receiving_traf_mode,\n" +
    "    null AS receiving_trade_mode,\n" +
    "    null AS receiving_trans_mode,\n" +
    "    null AS receiving_conta_id,\n" +
    "    null AS receiving_conta_model,\n" +
    "    null AS receiving_conta_wt,\n" +
    "    null AS receiving_haveconta,\n" +
    "    null AS receiving_eda_date,\n" +
    "    null AS receiving_region_0,\n" +
    "    null AS receiving_region_1,\n" +
    "    null AS receiving_region_2,\n" +
    "    null AS receiving_date_release,\n" +
    "    null AS receiving_new_add_sku_species,\n" +
    "    date_format(receiving_add_time, 'yyyyMM') AS month\n" +
    "FROM (SELECT * FROM ods.gc_wms_gc_receiving WHERE data_flag != 'DELETE') AS rc\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("gc_wms_warehouse","wa")} on wa.warehouse_code = rc.warehouse_code\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("gc_wms_customer","cu")} on cu.customer_code = rc.customer_code\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'income_type'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_income_type ON rc.income_type = v_income_type.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'receiving_shipping_type'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_receiving_shipping_type ON rc.receiving_shipping_type = v_receiving_shipping_type.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'receiving_type'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_receiving_type ON rc.receiving_type = v_receiving_type.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'receiving_status'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_receiving_status ON rc.receiving_status = v_receiving_status.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'create_type'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_create_type ON rc.create_type = v_create_type.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'rv_sign_status'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_rv_sign_status ON rc.rv_sign_status = v_rv_sign_status.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'is_send_email'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_is_send_email ON rc.is_send_email = v_is_send_email.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'email_result'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_email_result ON rc.email_result = v_email_result.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'is_diff'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_is_diff ON rc.is_diff = v_is_diff.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'diff_sure_status'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_diff_sure_status ON rc.diff_sure_status = v_diff_sure_status.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'vat_status'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_vat_status ON rc.vat_status = v_vat_status.vl_value\n" +
    "\n" +
    "LEFT JOIN (\n" +
    "  SELECT * FROM dw.par_val_list\n" +
    "  WHERE datasource_num_id = '9004'\n" +
    "  AND vl_type = 'file_status'\n" +
    "  AND vl_datasource_table = 'gc_wms_gc_receiving'\n" +
    ") AS v_file_status ON rc.file_status = v_file_status.vl_value"

  private val zy_wms = "SELECT" +
    "     rc.row_wid AS row_wid," +
    "     cast(from_unixtime( unix_timestamp( current_date ( ) ), 'yyyyMMdd' ) AS string ) AS etl_proc_wid," +
    "     current_timestamp ( ) AS w_insert_dt," +
    "     current_timestamp ( ) AS w_update_dt," +
    "     rc.datasource_num_id AS datasource_num_id," +
    "     rc.data_flag AS data_flag," +
    "     rc.receiving_id AS integration_id," +
    "     rc.created_on_dt AS created_on_dt," +
    "     rc.changed_on_dt AS changed_on_dt," +
    "     0 AS timezone," +
    "     0.00 AS exchange_rate," +
    "     concat(rc.datasource_num_id, sm.sm_id) AS sm_key," +
    "     concat(rc.datasource_num_id, rc.transit_warehouse_id) AS transit_warehouse_key," +
    "     concat(rc.datasource_num_id, rc.warehouse_id) AS warehouse_key," +
    "     concat(rc.datasource_num_id, rc.to_warehouse_id) AS to_warehouse_key," +
    "     concat(rc.datasource_num_id, rc.customer_id) AS customer_key," +
    "     rc.receiving_id AS receiving_id," +
    "     sm.sm_id AS receiving_sm_id," +
    "     rc.sm_code AS receiving_sm_code," +
    "     rc.receiving_code AS receiving_code," +
    "     rc.tracking_number AS receiving_tracking_number," +
    "     rc.shipping_method AS receiving_shipping_method,  " +
    "     rc.income_type AS receiving_income_type," +
    "     nvl(v_income_type.vl_bi_name, rc.income_type) AS receiving_income_type_val," +
    "     rc.po_code AS receiving_po_code," +
    "     rc.reference_no AS receiving_reference_no," +
    "     rc.transit_warehouse_id AS receiving_transit_warehouse_id," +
    "     null AS receiving_transit_warehouse_code," +
    "     rc.warehouse_id AS receiving_warehouse_id," +
    "     null AS receiving_warehouse_code," +
    "     rc.to_warehouse_id AS receiving_to_warehouse_id," +
    "     null AS receiving_to_warehouse_code," +
    "     rc.supplier_id AS receiving_supplier_id," +
    "     rc.receiving_update_user AS receiving_update_user," +
    "     rc.receiving_add_user AS receiving_add_user," +
    "     rc.customer_id AS  receiving_customer_id," +
    "     rc.customer_code AS receiving_customer_code," +
    "     rc.receiving_type AS receiving_type," +
    "     nvl(v_receiving_type.vl_bi_name, rc.receiving_type) AS receiving_type_val," +
    "     rc.receiving_status AS receiving_status," +
    "     nvl(v_receiving_status.vl_bi_name, rc.receiving_status) AS receiving_status_val," +
    "     rc.receiving_source_type AS receiving_source_type," +
    "     rc.contacter AS receiving_contacter," +
    "     rc.contact_phone AS receiving_contact_phone," +
    "     rc.receiving_description AS receiving_description," +
    "     rc.receiving_batch AS receiving_batch," +
    "     rc.receiving_add_time AS receiving_add_time," +
    "     rc.receiving_update_time AS receiving_update_time," +
    "     rc.total_packages AS receiving_total_packages," +
    "     rc.expected_date AS receiving_expected_date,  " +
    "     rc.receiving_transfer_status AS receiving_transfer_status," +
    "     nvl(v_receiving_transfer_status.vl_bi_name, rc.receiving_transfer_status) AS receiving_transfer_status_val," +
    "     rc.receiving_exception AS receiving_exception," +
    "     rc.receiving_exception_handle AS receiving_exception_handle," +
    "     rc.receiving_abnormal_confirm AS receiving_abnormal_confirm," +
    "     rc.tax_type AS receiving_tax_type," +
    "     nvl(v_tax_type.vl_bi_name, rc.tax_type) AS  receiving_tax_type_val," +
    "     rc.insurance_type AS receiving_insurance_type," +
    "     rc.customer_type AS receiving_customer_type," +
    "     nvl(v_customer_type.vl_bi_name, rc.customer_type) AS receiving_customer_type_val," +
    "     rc.wms_qty_confirm AS receiving_wms_qty_confirm," +
    "     rc.transit_receiving_code AS receiving_transit_receiving_code," +
    "     rc.sync_owms_status AS receiving_sync_owms_status," +
    "     nvl(v_sync_owms_status.vl_bi_name, rc.sync_owms_status) AS receiving_sync_owms_status_val," +
    "     rc.sync_owms_sign  AS receiving_sync_owms_sign," +
    "     nvl(v_sync_owms_sign.vl_bi_name, rc.sync_owms_sign) AS receiving_sync_owms_sign_val," +
    "     rc.sync_owms_time AS receiving_sync_owms_time," +
    "     rc.check_status AS receiving_check_status," +
    "     rc.is_fba AS receiving_is_fba," +
    "     rc.create_type AS receiving_create_type," +
    "     nvl(v_create_type.vl_bi_name, rc.create_type) AS receiving_create_type_val," +
    "     rc.is_return AS receiving_is_return," +
    "     rc.receiving_shipping_type AS receiving_shipping_type," +
    "     nvl(v_receiving_shipping_type.vl_bi_name, rc.receiving_shipping_type) AS receiving_shipping_type_val," +
    "     rc.received_packages AS received_packages," +
    "     rc.container_number AS receiving_container_number," +
    "     rc.first_sign_time AS receiving_first_sign_time," +
    "     rc.last_sign_time AS receiving_last_sign_time," +
    "     rc.rv_sign_batch AS receiving_rv_sign_batch," +
    "     rc.rv_sign_status AS receiving_rv_sign_status," +
    "     nvl(v_rv_sign_status.vl_bi_name, rc.rv_sign_status) AS  receiving_rv_sign_status_val," +
    "     rc.rv_timestamp AS receiving_rv_timestamp," +
    "     rc.volume AS receiving_volume," +
    "     rc.weight AS receiving_weight," +
    "     rc.is_fee_message AS receiving_is_fee_message," +
    "     rc.is_fee AS receiving_is_fee," +
    "     nvl(v_is_fee.vl_bi_name, rc.is_fee) AS receiving_is_fee_val,  " +
    "     rc.is_change_sku AS receiving_is_change_sku," +
    "     v_is_change_sku.vl_bi_name AS  receiving_is_change_sku_val," +
    "     rc.is_third AS receiving_is_third," +
    "     nvl(v_is_third.vl_bi_name, rc.is_third) AS receiving_is_third_val," +
    "     rc.take_stock_code AS receiving_take_stock_code," +
    "     rc.vat_number AS receiving_vat_number," +
    "     rc.exemption_number AS receiving_exemption_number," +
    "     rc.eori AS receiving_eori," +
    "     rc.auditing_user_id AS receiving_auditing_user_id," +
    "     rc.auditing_user_name AS receiving_auditing_user_name," +
    "     rc.reject_content AS receiving_reject_content," +
    "     rc.auditing_time AS receiving_auditing_time,  " +
    "     rc.vat_status AS receiving_vat_status," +
    "     nvl(v_vat_status.vl_bi_name, rc.vat_status) AS receiving_vat_status_val," +
    "     rc.c88_path AS receiving_c88_path," +
    "     rc.is_cost_fee AS receiving_is_cost_fee," +
    "     nvl(v_is_cost_fee.vl_bi_name, rc.is_cost_fee) AS receiving_is_cost_fee_val," +
    "     null AS receiving_reference_code," +
    "     null AS receiving_box_total," +
    "     null AS receiving_sku_total," +
    "     null AS receiving_sku_species," +
    "     null AS receiving_is_send_email," +
    "     null AS receiving_is_send_email_val," +
    "     null AS receiving_email_result," +
    "     null AS receiving_email_result_val," +
    "     null AS receiving_is_diff," +
    "     null AS receiving_is_diff_val," +
    "     null AS receiving_diff_first_time," +
    "     null AS receiving_diff_sure_time," +
    "     null AS receiving_diff_sure_status," +
    "     null AS receiving_diff_sure_status_val," +
    "     null AS receiving_country_code," +
    "     null AS receiving_state," +
    "     null AS receiving_city," +
    "     null AS receiving_district," +
    "     null AS receiving_street," +
    "     null AS receiving_audited_time," +
    "     null AS receiving_deliveried_time," +
    "     null AS receiving_shelved_time," +
    "     null AS receiving_file_status," +
    "     null AS receiving_file_status_val," +
    "     null AS receiving_wp_code," +
    "     null AS receiving_update_mark," +
    "     null AS receiving_transit_type," +
    "     null AS receiving_refercence_form_id," +
    "     null AS  receiving_ie_port," +
    "     null AS  receiving_form_type," +
    "     null AS  receiving_traf_name," +
    "     null AS  receiving_wrap_type," +
    "     null AS  receiving_pack_no," +
    "     null AS  receiving_traf_mode," +
    "     null AS  receiving_trade_mode," +
    "     null AS  receiving_trans_mode," +
    "     null AS  receiving_conta_id," +
    "     null AS  receiving_conta_model," +
    "     null AS  receiving_conta_wt," +
    "     null AS receiving_haveconta," +
    "     null AS receiving_eda_date," +
    "     null AS receiving_region_0," +
    "     null AS receiving_region_1," +
    "     null AS receiving_region_2," +
    "     null AS receiving_date_release," +
    "     null AS receiving_new_add_sku_species," +
    "     date_format(receiving_add_time, 'yyyyMM') AS month" +
    " FROM (SELECT * FROM ods.zy_wms_receiving WHERE data_flag != 'DELETE') AS rc" +
    s" LEFT JOIN ${Dw_dim_common.getDimSql("zy_wms_shipping_method","sm")} ON sm.sm_code = rc.sm_code " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_type = 'sm_code_type'" +
    "   AND vl_datasource_table = ''" +
    " ) AS v_sm_code_type ON sm.sm_code_type = v_sm_code_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'sync_owms_status'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_sync_owms_status ON rc.sync_owms_status = v_sync_owms_status.vl_value  " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'sync_owms_sign'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_sync_owms_sign ON rc.sync_owms_sign = v_sync_owms_sign.vl_value   " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'create_type'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_create_type ON rc.create_type = v_create_type.vl_value       " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'receiving_shipping_type'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_receiving_shipping_type ON rc.receiving_shipping_type = v_receiving_shipping_type.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'rv_sign_status'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_rv_sign_status ON rc.rv_sign_status = v_rv_sign_status.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'is_fee'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_is_fee ON rc.is_fee = v_is_fee.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'is_change_sku'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_is_change_sku ON rc.is_change_sku = v_is_change_sku.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'is_third'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_is_third ON rc.is_third = v_is_third.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'vat_status'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_vat_status ON rc.vat_status = v_vat_status.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'is_cost_fee'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_is_cost_fee ON rc.is_cost_fee = v_is_cost_fee.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'income_type'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_income_type ON rc.income_type = v_income_type.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'receiving_transfer_status'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_receiving_transfer_status ON rc.receiving_transfer_status = v_receiving_transfer_status.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'tax_type'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_tax_type ON rc.tax_type = v_tax_type.vl_value " +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'customer_type'" +
    "   AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_customer_type ON rc.customer_type = v_customer_type.vl_value" +
    "  LEFT JOIN (" +
    "  SELECT * FROM dw.par_val_list" +
    "  WHERE datasource_num_id = '9022'" +
    "  AND vl_type = 'receiving_status'" +
    "  AND vl_datasource_table = 'zy_wms_receiving'" +
    " ) AS v_receiving_status ON rc.receiving_status = v_receiving_status.vl_value" +
    " LEFT JOIN (" +
    "  SELECT * FROM dw.par_val_list" +
    "  WHERE datasource_num_id = '9022'" +
    "  AND vl_type = 'receiving_type'" +
    "  AND vl_datasource_table = 'zy_wms_receiving'" +
    "  ) AS v_receiving_type ON rc.receiving_type = v_receiving_type.vl_value"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_oms, gc_wms, zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))

    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqlArray, Array(SystemCodeUtil.GC_OMS, SystemCodeUtil.GC_WMS, SystemCodeUtil.ZY_WMS))
  }


}
