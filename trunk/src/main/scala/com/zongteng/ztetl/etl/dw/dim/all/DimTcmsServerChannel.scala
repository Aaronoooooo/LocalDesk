package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimTcmsServerChannel {
  //任务名称(一般同类名)
  private val task = "DimTcmsServerChannel"

  //dw层类名
  private val tableName = "dim_tcms_server_channel"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_tcms="SELECT ssc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ssc.datasource_num_id as datasource_num_id" +
    " ,ssc.data_flag as data_flag" +
    " ,ssc.integration_id as integration_id" +
    " ,ssc.created_on_dt as created_on_dt" +
    " ,ssc.changed_on_dt as changed_on_dt" +
    " ,cast(concat(ssc.datasource_num_id,ssc.server_id) as bigint) as server_key" +
    " ,ssc.server_channelid as server_channel_id" +
    " ,ssc.server_id as server_channel_server_id" +
    " ,ssc.formal_code as server_channel_formal_code" +
    " ,ssc.server_product_code as server_channel_server_product_code" +
    " ,ssc.server_channel_cnname as server_channel_cnname" +
    " ,ssc.server_channel_enname as server_channel_enname" +
    " ,ssc.server_channel_srname as server_channel_srname" +
    " ,ssc.servicer_id as server_channel_servicer_id" +
    " ,ssc.server_channel_enable as server_channel_enable" +
    " ,nvl(pvl1.vl_bi_name,ssc.server_channel_enable) as server_channel_enable_val" +
    " ,ssc.tms_id as server_channel_tms_id" +
    " ,ssc.server_remark as server_channel_server_remark" +
    " ,ssc.is_support_forcerelease as server_channel_is_support_forcerelease" +
    " ,nvl(pvl2.vl_bi_name,ssc.is_support_forcerelease) as server_channel_is_support_forcerelease_val" +
    " ,ssc.is_api_return_label as server_channel_is_api_return_label" +
    " ,nvl(pvl3.vl_bi_name,ssc.is_api_return_label) as server_channel_is_api_return_label_val" +
    " ,ssc.is_api_return_trackNum as server_channel_is_api_return_trackNum" +
    " ,nvl(pvl4.vl_bi_name,ssc.is_api_return_trackNum) as server_channel_is_api_return_trackNum_val" +
    " ,ssc.sort_code as server_channel_sort_code" +
    " ,ssc.single_number_intercept_rules as server_channel_single_number_intercept_rules" +
    " ,ssc.package_weight as server_channel_package_weight" +
    " ,ssc.server_forecast_product_code as server_channel_server_forecast_product_code" +
    " ,ssc.last_update_time as server_channel_last_update_time" +
    " ,ssc.is_multi as server_channel_is_multi" +
    " ,ssc.chargemode as server_channel_chargemode" +
    " ,ssc.childMinAmount as server_channel_childMinAmount" +
    " ,ssc.is_api_cancel_result as server_channel_is_api_cancel_result" +
    " ,ssc.lms_is_bill as server_channel_lms_is_bill" +
    " ,nvl(pvl5.vl_bi_name,ssc.lms_is_bill) as server_channel_lms_is_bill_val" +
    " ,ssc.lms_channel_code as server_channel_lms_channel_code" +
    " FROM (select * from ods.gc_tcms_csi_servechannel where day="+ nowDate +" ) as ssc" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='server_channel_enable' and pvl.vl_datasource_table='gc_tcms_csi_servechannel') as pvl1 on pvl1.vl_value=ssc.server_channel_enable" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='is_support_forcerelease' and pvl.vl_datasource_table='gc_tcms_csi_servechannel') as pvl2 on pvl2.vl_value=ssc.is_support_forcerelease" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='is_api_return_label' and pvl.vl_datasource_table='gc_tcms_csi_servechannel') as pvl3 on pvl3.vl_value=ssc.is_api_return_label" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='is_api_return_trackNum' and pvl.vl_datasource_table='gc_tcms_csi_servechannel') as pvl4 on pvl4.vl_value=ssc.is_api_return_trackNum" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='lms_is_bill' and pvl.vl_datasource_table='gc_tcms_csi_servechannel') as pvl5 on pvl5.vl_value=ssc.lms_is_bill"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_tcms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task, tableName, sqlArray, Array(SystemCodeUtil.GC_TCMS))
  }
}
