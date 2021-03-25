package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimTcmsCustomer {

  private val task = "Spark_Etl_Dim_Tcms_Customer"

  private val tableName = "dim_tcms_customer"

  private val nowDate: String = DateUtil.getNowTime()

  private val gc_tcms = "SELECT \n" +
    " row_wid AS row_wid,\n" +
    " date_format(current_date(), 'yyyyMMdd') AS etl_proc_wid,\n" +
    " current_timestamp ( ) AS w_insert_dt,\n" +
    " current_timestamp ( ) AS w_update_dt,\n" +
    " datasource_num_id AS datasource_num_id,\n" +
    " data_flag AS data_flag,\n" +
    " customer_id AS integration_id,\n" +
    " created_on_dt AS created_on_dt,\n" +
    " changed_on_dt AS changed_on_dt,\n" +
    " concat(datasource_num_id, og_id) AS og_key,\n" +
    " \n" +
    " customer_id AS customer_id,\n" +
    " customer_code AS customer_code,\n" +
    " customer_shortname AS customer_shortname,\n" +
    " customer_allname AS customer_allname,\n" +
    " customerstatus_code AS customer_customerstatus_code,\n" +
    " customerlevel_code AS customer_customerlevel_code,\n" +
    " customertype_code AS customer_customertype_code,\n" +
    " customersource_code AS customer_customersource_code,\n" +
    " nvl(v_customersource_code.vl_name, customersource_code) AS customer_customersource_code_val,\n" +
    " settlementtypes_code AS customer_settlementtypes_code,\n" +
    " nvl(v_settlementtypes_code.vl_name, settlementtypes_code) AS customer_settlementtypes_code_val,\n" +
    " customer_createdate AS customer_createdate,\n" +
    " customer_createrid AS customer_createrid,\n" +
    " server_status AS customer_server_status,\n" +
    " og_id AS customer_og_id,\n" +
    " tms_id AS customer_tms_id,\n" +
    " start_time AS customer_start_time,\n" +
    " sameday_time AS customer_sameday_time,\n" +
    " owe_intercept AS customer_owe_intercept,\n" +
    " nvl(v_owe_intercept.vl_name, owe_intercept) AS customer_owe_intercept_val,\n" +
    " owe_type AS customer_owe_type,\n" +
    " nvl(v_owe_type.vl_name, owe_type) AS customer_owe_type_val,\n" +
    " owe_intercept_date AS customer_owe_intercept_date,\n" +
    " pcs_code AS customer_pcs_code,\n" +
    " last_update_time AS customer_last_update_time,\n" +
    " orderreturnhandledays AS customer_orderreturnhandledays,\n" +
    " pickuppieces AS customer_pickuppieces,\n" +
    " orderabnormalhandledays AS customer_orderabnormalhandledays,\n" +
    " billcompany_id AS customer_billcompany_id,\n" +
    " bill_format AS customer_bill_format\n" +
    s"FROM (SELECT * FROM ods.gc_tcms_csi_customer WHERE day = '$nowDate') cus\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
    "  WHERE vl_datasource_table = 'gc_tcms_csi_customer' AND vl_type = 'customersource_code') v_customersource_code\n" +
    "ON cus.customersource_code = v_customersource_code.vl_value\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
    "  WHERE vl_datasource_table = 'gc_tcms_csi_customer' AND vl_type = 'settlementtypes_code') v_settlementtypes_code\n" +
    "ON cus.settlementtypes_code = v_settlementtypes_code.vl_value\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
    "  WHERE vl_datasource_table = 'gc_tcms_csi_customer' AND vl_type = 'owe_intercept') v_owe_intercept\n" +
    "ON cus.owe_intercept = v_owe_intercept.vl_value\n" +
    "\n" +
    "LEFT JOIN (SELECT vl_value, vl_name FROM dw.par_val_list \n" +
    "  WHERE vl_datasource_table = 'gc_tcms_csi_customer' AND vl_type = 'owe_type') v_owe_type\n" +
    "ON cus.owe_type = v_owe_type.vl_value"

  def main(args: Array[String]): Unit = {

    val sqlArray: Array[String] = Array(gc_tcms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task, tableName, Array(gc_tcms), Array(SystemCodeUtil.GC_TCMS))
  }

}
