package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimCustomer {

  //任务名称(一般同类名)
  private val task = "Dw_dim_customer"

  //dw层类名
  private val tableName = "dim_customer"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms = "select " +
    " cs.row_wid as row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') as etl_proc_wid, " +
    " current_timestamp() as w_insert_dt, " +
    " current_timestamp() as w_update_dt, " +
    " cs.datasource_num_id as datasource_num_id," +
    " cs.data_flag as data_flag," +
    " cs.integration_id as integration_id," +
    " cs.created_on_dt as created_on_dt," +
    " cs.changed_on_dt as changed_on_dt," +
    " cs.customer_id as customer_id," +
    " cs.customer_code as customer_code," +
    " cs.customer_short as customer_short," +
    " cs.customer_firstname as customer_firstname," +
    " cs.customer_lastname as customer_lastname," +
    " concat(cs.customer_firstname,cs.customer_lastname) as customer_name," +
    " cs.customer_email as customer_email," +
    " cs.cash_type as customer_cash_type, " +
    " nvl(val.vl_bi_name,cs.cash_type) as customer_cash_type_val," +
    " cs.customer_currency as customer_currency," +
    " cs.customer_telephone as customer_telephone," +
    " cs.customer_fax as customer_fax," +
    " cs.customer_status as customer_status," +
    " nvl(val2.vl_bi_name,cs.customer_status) as customer_status_val," +
    " cs.customer_company_name as customer_company_name," +
    " cs.customer_saler_user_id as customer_saler_user_id," +
    " cs.customer_cser_user_id as customer_cser_user_id," +
    " cs.customer_bill_date as customer_bill_date," +
    " cs.customer_signature as customer_signature," +
    " cs.customer_reg_time as customer_reg_time," +
    " cs.customer_note as customer_note," +
    " cb.cb_value  as customer_value," +
    " cb.cb_credit_line as customer_credit_line," +
    " cb.cb_update_time as customer_cb_update_time," +
    " cs.financial_customer_code as customer_financial_customer_code," +
    " cs.customer_product_verify as customer_product_verify," +
    " cs.subscribe_message as customer_subscribe_message," +
    " cs.frozen_status as customer_frozen_status," +
    " cs.email_subscribe_checked as customer_email_subscribe_checked," +
    " cs.register_source as customer_register_source," +
    " cs.sign_body as customer_sign_body," +
    " cs.ae_customer_code as customer_ae_customer_code," +
    " cs.verify_time as customer_verify_time," +
    " cs.versions as customer_versions," +
    " cs.customer_update_time as customer_update_time" +
    " FROM " +
    " (select * from ODS.gc_wms_customer where day="+ nowDate +")  AS cs" +
    " LEFT JOIN (select * from ODS.gc_wms_customer_balance where day="+ nowDate +") as cb on cb.customer_id=cs.customer_id" +
    " LEFT JOIN ( " +
    " SELECT * FROM dw.par_val_list " +
    " WHERE datasource_num_id = '9004' " +
    " AND vl_type = 'cash_type'" +
    " AND vl_datasource_table = 'gc_wms_customer' " +
    " ) AS val ON cs.cash_type = val.vl_value" +
    " LEFT JOIN ( " +
    " SELECT * FROM dw.par_val_list " +
    " WHERE datasource_num_id = '9004' " +
    " AND vl_type = 'customer_status'" +
    " AND vl_datasource_table = 'gc_wms_customer' " +
    " ) AS val2 ON cs.customer_status = val2.vl_value"
  private val zy_wms = "select" +
    " cs.row_wid as row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') as etl_proc_wid, " +
    " current_timestamp() as w_insert_dt, " +
    " current_timestamp() as w_update_dt, " +
    " cs.datasource_num_id as datasource_num_id," +
    " cs.data_flag as data_flag," +
    " cs.integration_id as integration_id," +
    " cs.created_on_dt as created_on_dt," +
    " cs.changed_on_dt as changed_on_dt, " +
    " cs.customer_id as customer_id," +
    " cs.customer_code as customer_code," +
    " cs.customer_short as customer_short," +
    " cs.customer_firstname as customer_firstname," +
    " cs.customer_lastname as customer_lastname," +
    " concat(cs.customer_firstname,cs.customer_lastname) as customer_name," +
    " cs.customer_email as customer_email," +
    " cs.cash_type as customer_cash_type," +
    " nvl(val.vl_bi_name,cs.cash_type) as customer_cash_type_val," +
    " cs.customer_currency as customer_currency," +
    " cs.customer_telephone as customer_telephone," +
    " cs.customer_fax as customer_fax," +
    " cs.customer_status as customer_status," +
    " nvl(val2.vl_bi_name,cs.customer_status) as customer_status_val," +
    " cs.customer_company_name as customer_company_name," +
    " cs.customer_saler_user_id as customer_saler_user_id," +
    " cs.customer_cser_user_id as customer_cser_user_id," +
    " cs.customer_bill_date as customer_bill_date," +
    " cs.customer_signature as customer_signature," +
    " cs.customer_reg_time as customer_reg_time," +
    " cs.customer_note as customer_note," +
    " cb.cb_value as customer_value," +
    " cb.cb_credit_line as customer_credit_line," +
    " cb.cb_update_time as customer_cb_update_time," +
    " cs.financial_customer_code as customer_financial_customer_code," +
    " cs.customer_product_verify as customer_product_verify," +
    " cs.subscribe_message as customer_subscribe_message," +
    " cs.frozen_status as customer_frozen_status," +
    " cs.email_subscribe_checked as customer_email_subscribe_checked," +
    " cs.register_source as customer_register_source," +
    " null as customer_sign_body," +
    " null as customer_ae_customer_code," +
    " null as customer_verify_time," +
    " null as customer_versions," +
    " cs.customer_update_time as customer_update_time" +
    " FROM " +
    " (select * from ODS.zy_wms_customer where day="+ nowDate +") AS cs" +
    " LEFT JOIN (select * from ODS.zy_wms_customer_balance where day="+ nowDate +") as cb on cb.customer_id=cs.customer_id" +
    " LEFT JOIN ( " +
    " SELECT * FROM dw.par_val_list " +
    " WHERE datasource_num_id = '9022' " +
    " AND vl_type = 'cash_type'" +
    " AND vl_datasource_table = 'zy_wms_customer' " +
    " ) AS val ON cs.cash_type = val.vl_value" +
    " LEFT JOIN ( " +
    " SELECT * FROM dw.par_val_list " +
    " WHERE datasource_num_id = '9022' " +
    " AND vl_type = 'customer_status'" +
    " AND vl_datasource_table = 'zy_wms_customer' " +
    " ) AS val2 ON cs.customer_status = val2.vl_value"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms, zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task, tableName, sqlArray, Array(SystemCodeUtil.GC_WMS, SystemCodeUtil.ZY_WMS))

  }
}
