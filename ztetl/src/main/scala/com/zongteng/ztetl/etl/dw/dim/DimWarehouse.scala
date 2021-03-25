package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimWarehouse {
  //任务名称(一般同类名)
  private val task = "Dw_dim_warehouse"

  //dw层类名
  private val tableName = "dim_warehouse"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private var gc_oms = "SELECT ws.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ws.datasource_num_id as datasource_num_id" +
    " ,ws.data_flag as data_flag" +
    " ,ws.integration_id as integration_id" +
    " ,ws.created_on_dt as  created_on_dt" +
    " ,ws.changed_on_dt as changed_on_dt" +
    " ,ws.warehouse_id as warehouse_id" +
    " ,ws.warehouse_code as warehouse_code" +
    " ,ws.warehouse_desc as warehouse_desc" +
    " ,ws.warehouse_type as warehouse_type" +
    " ,nvl(pvl3.vl_bi_name,ws.warehouse_type) as warehouse_type_val" +
    " ,ws.warehouse_status as warehouse_status" +
    " ,nvl(pvl4.vl_bi_name,ws.warehouse_status) as warehouse_status_val" +
    " ,ws.warehouse_virtual as warehouse_virtual" +
    " ,nvl(pvl5.vl_bi_name,ws.warehouse_virtual) as warehouse_virtual_val" +
    " ,ws.warehouse_transfer as warehouse_transfer" +
    " ,nvl(pvl6.vl_bi_name,ws.warehouse_transfer) as warehouse_transfer_val" +
    " ,ws.country_id as warehouse_country_id" +
    " ,ws.country_code as warehouse_country_code" +
    " ,ws.state as warehouse_state" +
    " ,ws.city as warehouse_city" +
    " ,ws.contacter as warehouse_contacter" +
    " ,ws.company as warehouse_company" +
    " ,ws.phone_no as warehouse_phone_no" +
    " ,ws.street_address1 as warehouse_street_address1" +
    " ,ws.street_address2 as warehouse_street_address2" +
    " ,ws.street_number  as warehouse_street_number" +
    " ,ws.postcode as warehouse_postcode" +
    " ,ws.warehouse_proxy_code as warehouse_proxy_code" +
    " ,ws.timezone as warehouse_timezone" +
    " ,ws.value_added_tax as warehouse_is_value_added_tax" +
    " ,nvl(pvl1.vl_bi_name,ws.value_added_tax) as warehouse_is_value_added_tax_val" +
    " ,ws.warehouse_en as warehouse_en" +
    " ,ws.saving_time as warehouse_saving_time" +
    " ,nvl(pvl2.vl_bi_name,ws.saving_time) as warehouse_saving_time_val" +
    " ,ws.arrival_days as warehouse_arrival_days" +
    " ,ws.currency_code as warehouse_currency_code" +
    " ,ws.warehouse_add_time as warehouse_add_time" +
    " ,ws.warehouse_update_time as warehouse_update_time" +
    " FROM (select * from ods.gc_oms_warehouse where day= "+ nowDate +") as ws" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='value_added_tax' and pvl.vl_datasource_table='gc_oms_warehouse') as pvl1 on pvl1.vl_value=ws.value_added_tax" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='saving_time' and pvl.vl_datasource_table='gc_oms_warehouse') as pvl2 on pvl2.vl_value=ws.saving_time" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='warehouse_type' and pvl.vl_datasource_table='gc_oms_warehouse') as pvl3 on pvl3.vl_value=ws.warehouse_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='warehouse_status' and pvl.vl_datasource_table='gc_oms_warehouse') as pvl4 on pvl4.vl_value=ws.warehouse_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='warehouse_virtual' and pvl.vl_datasource_table='gc_oms_warehouse') as pvl5 on pvl5.vl_value=ws.warehouse_virtual" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9001' and pvl.vl_type='warehouse_transfer' and pvl.vl_datasource_table='gc_oms_warehouse') as pvl6 on pvl6.vl_value=ws.warehouse_transfer"
  private var gc_wms = "SELECT ws.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ws.datasource_num_id as datasource_num_id" +
    " ,ws.data_flag as data_flag" +
    " ,ws.integration_id as integration_id" +
    " ,ws.created_on_dt as  created_on_dt" +
    " ,ws.changed_on_dt as changed_on_dt" +
    " ,ws.warehouse_id as warehouse_id" +
    " ,ws.warehouse_code as warehouse_code" +
    " ,ws.warehouse_desc as warehouse_desc" +
    " ,ws.warehouse_type as warehouse_type" +
    " ,nvl(pvl3.vl_bi_name,ws.warehouse_type) as warehouse_type_val" +
    " ,ws.warehouse_status as warehouse_status" +
    " ,nvl(pvl4.vl_bi_name,ws.warehouse_status) as warehouse_status_val" +
    " ,ws.warehouse_virtual as warehouse_virtual" +
    " ,nvl(pvl5.vl_bi_name,ws.warehouse_virtual) as warehouse_virtual_val" +
    " ,ws.warehouse_transfer as warehouse_transfer" +
    " ,nvl(pvl6.vl_bi_name,ws.warehouse_transfer) as warehouse_transfer_val" +
    " ,ws.country_id as warehouse_country_id" +
    " ,ws.country_code as warehouse_country_code" +
    " ,ws.state as warehouse_state" +
    " ,ws.city as warehouse_city" +
    " ,ws.contacter as warehouse_contacter" +
    " ,ws.company as warehouse_company" +
    " ,ws.phone_no as warehouse_phone_no" +
    " ,ws.street_address1 as warehouse_street_address1" +
    " ,ws.street_address2 as warehouse_street_address2" +
    " ,ws.street_number  as warehouse_street_number" +
    " ,ws.postcode as warehouse_postcode" +
    " ,ws.warehouse_proxy_code as warehouse_proxy_code" +
    " ,ws.timezone as warehouse_timezone" +
    " ,ws.value_added_tax as warehouse_is_value_added_tax" +
    " ,nvl(pvl1.vl_bi_name,ws.value_added_tax) as warehouse_is_value_added_tax_val" +
    " ,ws.warehouse_en as warehouse_en" +
    " ,ws.saving_time as warehouse_saving_time" +
    " ,nvl(pvl2.vl_bi_name,ws.saving_time) as warehouse_saving_time_val" +
    " ,ws.arrival_days as warehouse_arrival_days" +
    " ,ws.currency_code as warehouse_currency_code" +
    " ,ws.warehouse_add_time as warehouse_add_time" +
    " ,ws.warehouse_update_time as warehouse_update_time" +
    " FROM (select * from ods.gc_wms_warehouse where day= "+ nowDate +") as ws" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='value_added_tax' and pvl.vl_datasource_table='gc_wms_warehouse') as pvl1 on pvl1.vl_value=ws.value_added_tax" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='saving_time' and pvl.vl_datasource_table='gc_wms_warehouse') as pvl2 on pvl2.vl_value=ws.saving_time" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='warehouse_type' and pvl.vl_datasource_table='gc_wms_warehouse') as pvl3 on pvl3.vl_value=ws.warehouse_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='warehouse_status' and pvl.vl_datasource_table='gc_wms_warehouse') as pvl4 on pvl4.vl_value=ws.warehouse_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='warehouse_virtual' and pvl.vl_datasource_table='gc_wms_warehouse') as pvl5 on pvl5.vl_value=ws.warehouse_virtual" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='warehouse_transfer' and pvl.vl_datasource_table='gc_wms_warehouse') as pvl6 on pvl6.vl_value=ws.warehouse_transfer"
  private var zy_wms = "SELECT ws.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ws.datasource_num_id as datasource_num_id" +
    " ,ws.data_flag as data_flag" +
    " ,ws.integration_id as integration_id" +
    " ,ws.created_on_dt as  created_on_dt" +
    " ,ws.changed_on_dt as changed_on_dt" +
    " ,ws.warehouse_id as warehouse_id" +
    " ,ws.warehouse_code as warehouse_code" +
    " ,ws.warehouse_desc as warehouse_desc" +
    " ,ws.warehouse_type as warehouse_type" +
    " ,nvl(pvl3.vl_bi_name,ws.warehouse_type) as warehouse_type_val" +
    " ,ws.warehouse_status as warehouse_status" +
    " ,nvl(pvl4.vl_bi_name,ws.warehouse_status) as warehouse_status_val" +
    " ,ws.warehouse_virtual as warehouse_virtual" +
    " ,nvl(pvl5.vl_bi_name,ws.warehouse_virtual) as warehouse_virtual_val" +
    " ,ws.warehouse_transfer as warehouse_transfer" +
    " ,nvl(pvl6.vl_bi_name,ws.warehouse_transfer) as warehouse_transfer_val" +
    " ,ws.country_id as warehouse_country_id" +
    " ,ws.country_code as warehouse_country_code" +
    " ,ws.state as warehouse_state" +
    " ,ws.city as warehouse_city" +
    " ,ws.contacter as warehouse_contacter" +
    " ,ws.company as warehouse_company" +
    " ,ws.phone_no as warehouse_phone_no" +
    " ,ws.street_address1 as warehouse_street_address1" +
    " ,ws.street_address2 as warehouse_street_address2" +
    " ,ws.street_number  as warehouse_street_number" +
    " ,ws.postcode as warehouse_postcode" +
    " ,ws.warehouse_proxy_code as warehouse_proxy_code" +
    " ,ws.timezone as warehouse_timezone" +
    " ,ws.value_added_tax as warehouse_is_value_added_tax" +
    " ,nvl(pvl1.vl_bi_name,ws.value_added_tax) as warehouse_is_value_added_tax_val" +
    " ,null as warehouse_en" +
    " ,null as warehouse_saving_time" +
    " ,null as warehouse_saving_time_val" +
    " ,null as warehouse_arrival_days" +
    " ,null as warehouse_currency_code" +
    " ,ws.warehouse_add_time as warehouse_add_time" +
    " ,ws.warehouse_update_time as warehouse_update_time" +
    " FROM (select * from ods.zy_wms_warehouse where day= "+ nowDate +") as ws" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='value_added_tax' and pvl.vl_datasource_table='zy_wms_warehouse') as pvl1 on pvl1.vl_value=ws.value_added_tax" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='warehouse_type' and pvl.vl_datasource_table='zy_wms_warehouse') as pvl3 on pvl3.vl_value=ws.warehouse_type" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='warehouse_status' and pvl.vl_datasource_table='zy_wms_warehouse') as pvl4 on pvl4.vl_value=ws.warehouse_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='warehouse_virtual' and pvl.vl_datasource_table='zy_wms_warehouse') as pvl5 on pvl5.vl_value=ws.warehouse_virtual" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='warehouse_transfer' and pvl.vl_datasource_table='zy_wms_warehouse') as pvl6 on pvl6.vl_value=ws.warehouse_transfer"


  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_oms,gc_wms,zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task,tableName,sqlArray,Array(SystemCodeUtil.GC_OMS,SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))
  }

}
