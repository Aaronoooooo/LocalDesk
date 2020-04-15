package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimServer {
  //任务名称(一般同类名)
  private val task = "DimServer"

  //dw层类名
  private val tableName = "dim_server"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()
  //要执行的sql语句
  private val gc_wms ="SELECT csp.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,csp.datasource_num_id as datasource_num_id" +
    " ,csp.data_flag as data_flag" +
    " ,csp.integration_id as integration_id" +
    " ,csp.created_on_dt as created_on_dt" +
    " ,csp.changed_on_dt as changed_on_dt" +
    " ,csp.sp_id as server_id" +
    " ,csp.sp_code as server_code" +
    " ,csp.sp_name as server_name" +
    " ,csp.sp_contact_name as server_contact_name" +
    " ,csp.sp_contact_phone as server_contact_phone" +
    " ,csp.sp_address as server_address" +
    " ,csp.sp_settlement_type as server_settlement_type" +
    " ,nvl(pvl1.vl_bi_name,csp.sp_settlement_type) as server_settlement_type_val" +
    " ,csp.sp_balance as server_balance" +
    " ,csp.currency_code as server_currency_code" +
    " ,csp.sp_tax as server_tax" +
    " ,csp.sp_person_user as server_person_user" +
    " ,csp.sp_update_date as server_update_date" +
    " ,csp.sp_type as server_type" +
    " ,csp.is_ignore_express as server_is_ignore_express" +
    " ,csp.as_id as server_as_id" +
    " ,ass.as_name as server_as_name" +
    " ,csp.sp_add_date as server_add_date" +
    " ,csp.sp_status as server_status" +
    " ,nvl(pvl2.vl_bi_name,csp.sp_status) as server_status_val" +
    " ,csp.ap_id as server_ap_id" +
    " ,csp.carrier_name as server_carrier_name" +
    " ,csp.sp_cancel_label_flag as server_cancel_label_flag" +
    " ,nvl(pvl3.vl_bi_name,csp.sp_cancel_label_flag) as server_is_cancel_label_flag_val" +
    " FROM (select * from ods.gc_wms_service_provider where day= "+ nowDate +" ) as csp" +
    " left JOIN (select * from ods.gc_wms_api_service where day= "+ nowDate +" ) as ass on ass.as_id=csp.as_id" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='sp_settlement_type' and pvl.vl_datasource_table='gc_wms_service_provider') as pvl1 on pvl1.vl_value=csp.sp_settlement_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='sp_status' and pvl.vl_datasource_table='gc_wms_service_provider') as pvl2 on pvl2.vl_value=csp.sp_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='sp_cancel_label_flag' and pvl.vl_datasource_table='gc_wms_service_provider') as pvl3 on pvl3.vl_value=csp.sp_cancel_label_flag"
  private val zy_wms ="SELECT csp.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,csp.datasource_num_id as datasource_num_id" +
    " ,csp.data_flag as data_flag" +
    " ,csp.integration_id as integration_id" +
    " ,csp.created_on_dt as created_on_dt" +
    " ,csp.changed_on_dt as changed_on_dt" +
    " ,csp.sp_id as server_id" +
    " ,csp.sp_code as server_code" +
    " ,csp.sp_name as server_name" +
    " ,csp.sp_contact_name as server_contact_name" +
    " ,csp.sp_contact_phone as server_contact_phone" +
    " ,csp.sp_address as server_address" +
    " ,csp.sp_settlement_type as server_settlement_type" +
    " ,nvl(pvl1.vl_bi_name,csp.sp_settlement_type) as server_settlement_type_val" +
    " ,csp.sp_balance as server_balance" +
    " ,csp.currency_code as server_currency_code" +
    " ,csp.sp_tax as server_tax" +
    " ,csp.sp_person_user as server_person_user" +
    " ,csp.sp_update_date as server_update_date" +
    " ,csp.sp_type as server_type" +
    " ,csp.is_ignore_express as server_is_ignore_express" +
    " ,csp.as_id as server_as_id" +
    " ,ass.as_name as server_as_name" +
    " ,csp.sp_add_date as server_add_date" +
    " ,csp.sp_status as server_status" +
    " ,nvl(pvl2.vl_bi_name,csp.sp_status) as server_status_val" +
    " ,csp.ap_id as server_ap_id" +
    " ,csp.carrier_name as server_carrier_name" +
    " ,csp.sp_cancel_label_flag as server_cancel_label_flag" +
    " ,nvl(pvl3.vl_bi_name,csp.sp_cancel_label_flag) as server_is_cancel_label_flag_val" +
    " FROM (select * from ods.zy_wms_service_provider where day= "+ nowDate +" ) as csp" +
    " left JOIN (select * from ods.zy_wms_api_service where day= "+ nowDate +" ) as ass on ass.as_id=csp.as_id" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='sp_settlement_type' and pvl.vl_datasource_table='zy_wms_service_provider') as pvl1 on pvl1.vl_value=csp.sp_settlement_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='sp_status' and pvl.vl_datasource_table='zy_wms_service_provider') as pvl2 on pvl2.vl_value=csp.sp_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='sp_cancel_label_flag' and pvl.vl_datasource_table='zy_wms_service_provider') as pvl3 on pvl3.vl_value=csp.sp_cancel_label_flag"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms,zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task,tableName,sqlArray,Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))
  }
}
