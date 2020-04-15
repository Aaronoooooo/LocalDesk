package com.zongteng.ztetl.etl.dw.fact.full.transfer

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DW_fact_tcms_business_operator_full {

    //任务名称(一般同类名)
    private val task = "DW_fact_tcms_business_operator_full"

    //dw层类名
    private val tableName = "fact_tcms_business_operator"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val select_fact_tcms_business_operator =
        "select bo.row_wid, " +
          "from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid, " +
          "current_timestamp ( ) as w_insert_dt, " +
          "current_timestamp ( ) as w_update_dt, " +
          "bo.datasource_num_id as datasource_num_id, " +
          "bo.data_flag as data_flag, " +
          "integration_id as integration_id, " +
          "bo.created_on_dt as created_on_dt, " +
          "bo.changed_on_dt as changed_on_dt, " +
          "null AS timezone , " +
          "null AS exchange_rate," +
          "CONCAT(bo.datasource_num_id, bo.checkin_st_id)  as checkin_st_key, " +
          "CONCAT(bo.datasource_num_id, bo.checkout_st_id)  as checkout_st_key, "+
          "bo.bs_id," +
          "bo.checkin_date," +
          "bo.checkin_st_id," +
          "bo.entry_date," +
          "bo.entry_st_id," +
          "bo.checkout_date," +
          "bo.checkout_st_id," +
          "bo.last_track_code," +
          "bo.last_track_date," +
          "bo.last_track_location," +
          "bo.last_track_comment," +
          "bo.last_comment," +
          "bo.st_id_comment," +
          "bo.last_comment_date," +
          "bo.last_documentary," +
          "bo.st_id_documentary," +
          "bo.last_documentary_date," +
          "bo.last_update_time," +
          "date_format(bo.w_insert_dt,'yyyyMM') as month" +
          " from " +
          " (select * from ods.gc_tcms_bsn_business_operator where data_flag <> 'delete') as bo"

    def main(args: Array[String]): Unit = {
        println(select_fact_tcms_business_operator)
       // val sqlArray: Array[String] = Array(select_fact_tcms_business_operator).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
        Dw_fact_common.getRunCode_hive_full_Into(task,tableName, Array(select_fact_tcms_business_operator))
    }

}
