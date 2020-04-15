package com.zongteng.ztetl.etl.dw.fact.full.transfer

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DW_fact_tcms_business_full {

    //任务名称(一般同类名)
    private val task = "DW_fact_tcms_business_full"

    //dw层类名
    private val tableName = "fact_tcms_business"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val create_fact_tcms_business =
        "select bb.row_wid, " +
          "from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid, " +
          "current_timestamp ( ) as w_insert_dt, " +
          "current_timestamp ( ) as w_update_dt, " +
          "bb.datasource_num_id as datasource_num_id, " +
          "bb.data_flag as data_flag, " +
          "integration_id as integration_id, " +
          "bb.created_on_dt as created_on_dt, " +
          "bb.changed_on_dt as changed_on_dt, " +
          "null AS timezone , " +
          "null AS exchange_rate," +
          " CONCAT(bb.datasource_num_id, bb.customer_id)  as customer_key, " +
          "CONCAT(bb.datasource_num_id, bb.destination_cityid)  as country_key, " +
          "bb.tms_id, " +
          "bb.bs_id, " +
          "bb.arrivalbatch_id, " +
          "bb.checkin_og_id, " +
          "bb.datasource_code, " +
          "bb.transferstatus_code, " +
          "bb.returnstatus_code, " +
          "bb.operation_status, " +
          "bb.documentarystatus_code, " +
          "bb.customer_id, " +
          "bb.customer_channelid, " +
          "bb.arrival_date," +
          "bb.product_code," +
          "bb.checkin_cargotype, " +
          "bb.paymentmode_code, " +
          "bb.destination_countrycode, " +
          "bb.destination_cityid, " +
          "bb.destination_postcode, " +
          "bb.shipper_weight, " +
          "bb.checkin_grossweight, " +
          "bb.checkin_volumeweight, " +
          "bb.shipper_chargeweight, " +
          "bb.estimate_chargeweight, " +
          "bb.server_chargeweight, " +
          "bb.shipper_pieces, " +
          "bb.server_channelid, " +
          "bb.checkout_cargotype, " +
          "bb.departure_date, " +
          "bb.prealert_date, " +
          "bb.return_sign, " +
          "bb.lastoperate_og_id, " +
          "bb.transaction_id, " +
          "bb.warehouse_code, " +
          "bb.seller_id, " +
          "bb.departbatch_id, " +
          "bb.checkout_grossweight, " +
          "bb.checkout_volumeweight, " +
          "bb.mail_cargo_type, " +
          "bb.serve_weight_checksign, " +
          "bb.invoice_totalcharge, " +
          "bb.manual_shipper_chargeweight, " +
          "bb.manual_server_chargeweight, " +
          "bb.virtual_business, " +
          "bb.unit_code, bb.return_time, " +
          "bb.return_note, " +
          "bb.sysn_state, " +
          "bb.input_mode, " +
          "bb.manual_return_fee_sign, " +
          "bb.manual_return_fee_date, " +
          "bb.sync_fba, " +
          "bb.points_bubble_ratio, " +
          "bb.manual_return_fee_Accrued_sign, " +
          "bb.return_status_audit_state, " +
          "bb.volume_coefficient, " +
          "bb.volume_state, " +
          "bb.last_update_time, " +
          "bb.trackingnumber_main, " +
          "bb.boxnumber, " +
          "bb.server_change, " +
          "bb.zd_id, bb.zs_id, " +
          "bb.organization_id, " +
          "bb.packing_id, " +
          "bb.address_type, " +
          "bb.main_orderid, " +
          "nvl(pv11.vl_bi_name,bb.datasource_code), " +
          "nvl(pv12.vl_bi_name,bb.paymentmode_code), " +
          "nvl(pv13.vl_bi_name,bb.input_mode), " +
          "nvl(pv14.vl_bi_name,bb.address_type), " +
          "date_format(bb.w_insert_dt,'yyyyMM') as month" +
          " from " +
          "(select * from ods.gc_tcms_bsn_business where data_flag <> 'delete')bb " +
          " left join " +
          " (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='datasource_code' and pvl.vl_datasource_table='gc_tcms_bsn_business')as pv11 " +
          " on pv11.vl_value = bb.datasource_code " +
          " left join " +
          " (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='paymentmode_code' and pvl.vl_datasource_table='gc_tcms_bsn_business')as pv12" +
          " on pv12.vl_value = bb.paymentmode_code" +
          " left join (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='input_mode' and pvl.vl_datasource_table='gc_tcms_bsn_business')as pv13 " +
          " on pv13.vl_value = bb.input_mode " +
          " left join " +
          " (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='address_type' and pvl.vl_datasource_table='gc_tcms_bsn_business')as pv14 " +
          " on pv14.vl_value = bb.address_type"
    def main(args: Array[String]): Unit = {

        val sqlArray: Array[String] = Array(create_fact_tcms_business).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
        Dw_fact_common.getRunCode_hive_full_Into(task,tableName,sqlArray,Array(SystemCodeUtil.GC_TCMS))
    }
}
