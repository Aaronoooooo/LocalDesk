package com.zongteng.ztetl.etl.dw.fact.full.transfer

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DW_fact_tcms_business_expressexport_full {
    //任务名称(一般同类名)
    private val task = "DW_fact_tcms_business_full"

    //dw层类名
    private val tableName = "fact_tcms_business_expressexport"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val select_fact_tcms_business_expressexport =
        "select be.row_wid, " +
          "from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid, " +
          "current_timestamp ( ) as w_insert_dt, " +
          "current_timestamp ( ) as w_update_dt, " +
          "be.datasource_num_id as datasource_num_id, " +
          "be.data_flag as data_flag, " +
          "integration_id as integration_id, " +
          "be.created_on_dt as created_on_dt, " +
          "be.changed_on_dt as changed_on_dt, " +
          "null AS timezone , " +
          "null AS exchange_rate," +
          " CONCAT(be.datasource_num_id, be.lastselect_id)  as lastselect_key, " +
          "be.bs_id," +
          "be.shipper_hawbcode," +
          "be.serve_hawbcode," +
          "be.refer_hawbcode," +
          "be.small_labelcode," +
          "be.channel_hawbcode," +
          "be.original_invoicesign," +
          "be.invoice_referencesign," +
          "be.sender_account," +
          "be.payer_account," +
          "be.third_partyaccount," +
          "be.label_printtimes," +
          "be.lastselect_id," +
          "be.invoice_printdate," +
          "be.oda_sign," +
          "be.booking_code," +
          "be.booking_sign," +
          "be.manifest_sign," +
          "be.seekcargo_sign," +
          "be.battery_code," +
          "be.tariffno," +
          "be.printformat_sign," +
          "be.buyer_id," +
          "be.collectcharge_confirmsign," +
          "be.document_change_sign," +
          "be.last_update_time,"+
          "nvl(pv11.vl_bi_name,be.oda_sign), " +
          "nvl(pv12.vl_bi_name,be.seekcargo_sign), " +
          "nvl(pv13.vl_bi_name,be.printformat_sign), " +
          "date_format(be.w_insert_dt,'yyyyMM') as month" +
          " from " +
          "(select * from ods.gc_tcms_bsn_expressexport where data_flag <> 'delete')be " +
          " left join " +
          " (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='oda_sign' and pvl.vl_datasource_table='gc_tcms_bsn_expressexport')as pv11 " +
          " on pv11.vl_value = be.oda_sign " +
          " left join " +
          " (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='seekcargo_sign' and pvl.vl_datasource_table='gc_tcms_bsn_expressexport')as pv12" +
          " on pv12.vl_value = be.seekcargo_sign" +
          " left join (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='printformat_sign' and pvl.vl_datasource_table='gc_tcms_bsn_expressexport')as pv13 " +
          " on pv13.vl_value = be.printformat_sign "
    def main(args: Array[String]): Unit = {
        val sqlArray: Array[String] = Array(select_fact_tcms_business_expressexport).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
        Dw_fact_common.getRunCode_hive_full_Into(task,tableName,sqlArray,Array(SystemCodeUtil.GC_TCMS))
    }
}
