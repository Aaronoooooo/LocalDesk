package com.zongteng.ztetl.etl.dw.fact.full.storage

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_product_warehouse_location_map_full {

    //任务名称(一般同类名)
    private val task = "Dw_fact_product_warehouse_location_map_full"

    //dw层类名
    private val tableName = "fact_product_warehouse_location_map"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val select_fact_product =
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
          "null AS exchange_rate,"+
          "plm.plm_id ," +
          " nvl(plm.product_id,0) ," +
          " nvl(plm.warehouse_id,0) ," +
          "nvl(dp.product_barcode,'')," +
          "nvl(dimw.warehouse_code,'')," +
          "nvl(plm.lc_code ,'')," +
          "plm_update_time ," +
          "date_format(bb.created_on_dt,'yyyyMM') as month " +
          " from " +
          "ods.gc_zy_wms_product_location_map plm " +
          "left join " +
          "dw.dim_product dp " +
          "on plm.product_id = dp.product_id " +
          "left join " +
          " dw.dim_warehouse dimw " +
          "on plm.warehouse_id = dimw.warehouse_id"

    def main(args: Array[String]): Unit = {

        val sqlArray: Array[String] = Array(select_fact_product).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
        Dw_fact_common.getRunCode_hive_full_Into(task,tableName,sqlArray,Array(SystemCodeUtil.GC_TCMS))
    }

}
