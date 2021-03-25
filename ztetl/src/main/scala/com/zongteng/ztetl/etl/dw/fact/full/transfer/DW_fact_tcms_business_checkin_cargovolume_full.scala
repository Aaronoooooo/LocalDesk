package com.zongteng.ztetl.etl.dw.fact.full.transfer

import com.zongteng.ztetl.common.Dw_fact_common
import com.zongteng.ztetl.util.DateUtil

object DW_fact_tcms_business_checkin_cargovolume_full {
    //任务名称(一般同类名)
    private val task = "DW_fact_tcms_business_checkin_cargovolume_full"

    //dw层类名
    private val tableName = "fact_tcms_business_checkin_cargovolume"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val select_fact_tcms_business_checkin_cargovolume =
        "select bcc.row_wid, " +
          "from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid, " +
          "current_timestamp ( ) as w_insert_dt, " +
          "current_timestamp ( ) as w_update_dt, " +
          "bcc.datasource_num_id as datasource_num_id, " +
          "bcc.data_flag as data_flag, " +
          "bcc.integration_id as integration_id, " +
          "bcc.created_on_dt as created_on_dt, " +
          "bcc.changed_on_dt as changed_on_dt, " +
          "null AS timezone , " +
          "null AS exchange_rate,"+
          "bcc.involume_id," +
          "bcc.bs_id," +
          "bcc.involume_length," +
          "bcc.involume_width," +
          "bcc.involume_height," +
          "bcc.involume_grossweight," +
          "bcc.involume_volumeweight," +
          "bcc.involume_chargeweight," +
          "bcc.package_weight," +
          "bcc.child_forecast_number," +
          "bcc.child_track_number," +
          "bcc.last_update_time,"+
          "date_format(bcc.w_insert_dt,'yyyyMM') as month" +
          " from " +
          " (select * from ods.gc_tcms_bsn_cargovolume where data_flag <> 'delete') as bcc"

    def main(args: Array[String]): Unit = {
        // val sqlArray: Array[String] = Array(select_fact_tcms_business_operator).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
        Dw_fact_common.getRunCode_hive_full_Into(task,tableName, Array(select_fact_tcms_business_checkin_cargovolume))
    }
}
