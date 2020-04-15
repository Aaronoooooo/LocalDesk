package com.zongteng.ztetl.etl.dw.fact.full.orders

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_oversea_abnormal_order_full {

  val selectSql = "SELECT \n" +
    "  od.row_wid AS row_wid,\n" +
    "  date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    "  current_timestamp ( ) AS w_insert_dt,\n" +
    "  current_timestamp ( ) AS w_update_dt,\n" +
    "  od.datasource_num_id AS datasource_num_id,\n" +
    "  od.data_flag AS data_flag,\n" +
    "  od.order_id AS integration_id,\n" +
    "  od.created_on_dt AS created_on_dt,\n" +
    "  od.changed_on_dt AS changed_on_dt,\n" +
    "  NULL AS timezone,\n" +
    "  NULL AS exchange_rate,\n" +
    "\n" +
    "  concat(od.datasource_num_id, od.customer_id) AS customer_key,\n" +
    "  concat(od.datasource_num_id, od.warehouse_id) AS warehouse_key,\n" +
    "  concat(od.datasource_num_id, sm.sm_id) AS sm_key,\n" +
    "  concat(od.datasource_num_id, sc.sc_id) AS customer_key,\n" +
    "\n" +
    "  od.order_id,\n" +
    "  od.platform AS order_platform,\n" +
    "  od.order_platform_type AS order_platform_type,\n" +
    "  od.create_type AS order_create_type,\n" +
    "  od.order_code,\n" +
    "  od.reference_no AS order_reference_no,\n" +
    "  od.tracking_number AS order_tracking_number,\n" +
    "  od.order_type,\n" +
    "  v_order_type.vl_name AS order_type_val,\n" +
    "  od.order_status,\n" +
    "  v_order_status.vl_name AS order_status_val,\n" +
    "  od.customer_code AS order_customer_code,\n" +
    "  NULL AS order_warehouse_code,\n" +
    "  od.sm_code AS order_sm_code,\n" +
    "  od.sc_code AS order_sc_code,\n" +
    "  od.parcel_quantity AS order_parcel_quantity,\n" +
    "  od.order_pick_type,\n" +
    "  v_order_pick_type.vl_name AS order_pick_type_val,\n" +
    "  od.picker_id AS order_pick_user_id,\n" +
    "  od.country_code  AS order_country_code,\n" +
    "\n" +
    "  NULL AS order_buyer_firstname,\n" +
    "  NULL AS order_buyer_lastname,\n" +
    "  NULL AS order_buyer_company,\n" +
    "\n" +
    "  od.order_exception_sub_type,\n" +
    "  od.order_exception_status,\n" +
    "  v_order_exception_status.vl_name AS order_exception_status_val,\n" +
    "  od.add_time AS order_add_time,\n" +
    "  od.update_time AS order_update_time,\n" +
    "\n" +
    "  NULL AS order_oot_update_time,\n" +
    "  NULL AS order_print_time,\n" +
    "  NULL AS order_pack_time,\n" +
    "  NULL AS order_abnorma_time,\n" +
    "  NULL AS order_abnormal_deal_time,\n" +
    "  NULL AS order_print_user_id,\n" +
    "  NULL AS order_pack_user_id,\n" +
    "  NULL AS order_abnormal_user_id,\n" +
    "  NULL AS order_abnormal_deal_user_id,\n" +
    "  date_format(od.add_time, 'yyyyMM') month\n"

  val fromSql = "FROM (SELECT * FROM ods.ordersTable WHERE data_flag != 'DELETE') od\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("smTable","sm")} ON od.sm_code = sm.sm_code\n" +
    s"LEFT JOIN ${Dw_dim_common.getDimSql("scTable","sc")} ON od.sc_code = sc.sc_code\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list WHERE vl_type = 'order_type' AND vl_datasource_table = 'ordersTable') v_order_type ON od.order_type = v_order_type.vl_value\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list WHERE vl_type = 'order_status' AND vl_datasource_table = 'ordersTable') v_order_status ON od.order_status = v_order_status.vl_value\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list WHERE vl_type = 'order_pick_type' AND vl_datasource_table = 'ordersTable') v_order_pick_type ON od.order_pick_type = v_order_pick_type.vl_value\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list WHERE vl_type = 'order_exception_status' AND vl_datasource_table = 'ordersTable') v_order_exception_status ON od.order_exception_status = v_order_exception_status.vl_value"

  private val tables = Array(
    "gc_owms_au_orders",
    "gc_owms_cz_orders",
    "gc_owms_de_orders",
    "gc_owms_es_orders",
    "gc_owms_frvi_orders",
    "gc_owms_it_orders",
    "gc_owms_jp_orders",
    "gc_owms_uk_orders",
    "gc_owms_ukob_orders",
    "gc_owms_usea_orders",
    "gc_owms_uswe_orders",
    "gc_owms_usnb_orders",
    "gc_owms_usot_orders",
    "gc_owms_ussc_orders",
    "zy_owms_au_orders",
    "zy_owms_cz_orders",
    "zy_owms_de_orders",
    "zy_owms_ru_orders",
    "zy_owms_uk_orders",
    "zy_owms_usea_orders",
    "zy_owms_uswe_orders",
    "zy_owms_ussc_orders"
  )

  val SystemCodes = Array(
    SystemCodeUtil.GC_OWMS_AU,
    SystemCodeUtil.GC_OWMS_CZ,
    SystemCodeUtil.GC_OWMS_DE,
    SystemCodeUtil.GC_OWMS_ES,
    SystemCodeUtil.GC_OWMS_FRVI,
    SystemCodeUtil.GC_OWMS_IT,
    SystemCodeUtil.GC_OWMS_JP,
    SystemCodeUtil.GC_OWMS_UK,
    SystemCodeUtil.GC_OWMS_UKOB,
    SystemCodeUtil.GC_OWMS_USEA,
    SystemCodeUtil.GC_OWMS_USWE,
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

  val task = "Dw_fact_oversea_abnormal_order"

  val tableName = "fact_oversea_abnormal_order"

  val nowDay = DateUtil.getNowTime()

  def makeSql() = {

    tables.map((table: String) => {

      val ordersTable = table
      val smTable = if(table.contains("gc_owms")) "gc_wms_shipping_method" else "zy_wms_shipping_method"
      val scTable = if(table.contains("gc_owms")) "gc_wms_sp_service_channel" else "zy_wms_sp_service_channel"

      selectSql + fromSql.replaceAll("ordersTable", ordersTable).
        replaceAll("smTable", smTable).
        replaceAll("scTable", scTable)
    })
  }

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = makeSql.map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqlArray, SystemCodes)
  }

}
