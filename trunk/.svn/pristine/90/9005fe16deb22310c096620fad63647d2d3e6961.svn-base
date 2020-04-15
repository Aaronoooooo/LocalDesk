package com.zongteng.ztetl.etl.dw.fact.full.storage

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_inventory_difference_detail_full {
    //任务名称(一般同类名)
    private val task = "Dw_fact_inventory_difference_detail_full"

    //dw层类名
    private val tableName = "fact_inventory_difference_detail"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    //要执行的sql语句
    private val gc_wms = "select idd.row_wid " +
      " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
      " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
      " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
      " ,idd.datasource_num_id as datasource_num_id" +
      " ,idd.data_flag as  data_flag" +
      " ,idd.integration_id as  integration_id" +
      " ,idd.created_on_dt as created_on_dt" +
      " ,idd.changed_on_dt as changed_on_dt" +
      " ,null as timezone" +
      " ,null as exchange_rate" +
      " ,concat(idd.datasource_num_id,id.warehouse_id) warehouse_key" +
      " ,concat(idd.datasource_num_id,idd.product_id) product_key" +
      " ,c.row_wid customer_key" +
      " ,wp.row_wid wp_key" +
      " ,idd.idi_id idd_idi_id" +
      " ,idd.di_id idd_di_id" +
      " ,idd.product_id idd_product_id" +
      " ,idd.product_barcode idd_product_barcode" +
      " ,c.customer_id  idd_customer_id " +
      " ,idd.customer_code idd_customer_code" +
      " ,idd.difference_type idd_difference_type" +
      " ,nvl(pvl1.vl_bi_name,idd.difference_type) as idd_difference_type_val" +
      " ,idd.difference_num idd_difference_num" +
      " ,idd.difference_actual_num idd_difference_actual_num" +
      " ,idd.di_status idd_di_status" +
      " ,nvl(pvl2.vl_bi_name,idd.di_status) as idd_di_status_val" +
      " ,idd.orgin_quantity idd_orgin_quantity" +
      " ,idd.difference_code idd_difference_code" +
      " ,idd.fv_id idd_fv_id" +
      " ,idd.operate_id idd_operate_id" +
      " ,idd.difference_note idd_difference_note" +
      " ,id.warehouse_id idd_warehouse_id" +
      " ,id.warehouse_code idd_warehouse_code" +
      " ,wp.wp_id idd_wp_id" +
      " ,wp.wp_code  idd_wp_code" +
      " from  (select * from ods.gc_wms_inventory_difference_detail where  data_flag<>'delete') as idd" +
      " left join (select * from ods.gc_wms_inventory_difference where  data_flag<>'delete') as id on id.di_id=idd.di_id" +
      s" left join ${Dw_dim_common.getDimSql("gc_wms_warehouse_physical","wp")} on wp.wp_code=id.wp_code" +
      s" left join ${Dw_dim_common.getDimSql("gc_wms_customer","c")} on c.customer_code=idd.customer_code" +
      " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='difference_type' and pvl.vl_datasource_table='gc_wms_inventory_difference_detail') " +
      "            as pvl1 on pvl1.vl_value=idd.difference_type" +
      " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='di_status' and pvl.vl_datasource_table='gc_wms_inventory_difference_detail') " +
      "            as pvl2 on pvl2.vl_value=idd.di_status"

    def main(args: Array[String]): Unit = {
      val sqlArray: Array[String] = Array(gc_wms).map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
      Dw_fact_common.getRunCode_hive_nopartition_full_Into(task,tableName,sqlArray,Array(SystemCodeUtil.GC_WMS))
    }
}
