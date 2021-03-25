package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object ParSearchFilter {
  //任务名称(一般同类名)
  private val task = "ParSearchFilter"

  //dw层类名
  private val tableName = "par_search_filter"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()
  //要执行的sql语句
  private val gc_wms ="SELECT sf.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,sf.datasource_num_id as datasource_num_id" +
    " ,sf.data_flag as data_flag" +
    " ,sf.integration_id as integration_id" +
    " ,sf.created_on_dt as  created_on_dt" +
    " ,sf.changed_on_dt as changed_on_dt" +
    " ,sf.sf_id as sf_id" +
    " ,sf.warehouse_id as warehouse_id" +
    " ,sf.parent_id as parent_id" +
    " ,sf.search_label as search_label" +
    " ,sf.search_label_en as search_label_en" +
    " ,sf.search_label_ru as search_label_ru" +
    " ,sf.search_value as search_value" +
    " ,sf.search_sort as search_sort" +
    " ,sf.filter_action_id as filter_action_id" +
    " ,sf.search_tips as search_tips" +
    " ,sf.search_input_id as search_input_id" +
    " ,sf.sf_desc as sf_desc" +
    " FROM (select * from ods.gc_wms_search_filter where day= "+ nowDate +") as sf"
  private val zy_wms ="SELECT sf.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,sf.datasource_num_id as datasource_num_id" +
    " ,sf.data_flag as data_flag" +
    " ,sf.integration_id as integration_id" +
    " ,sf.created_on_dt as  created_on_dt" +
    " ,sf.changed_on_dt as changed_on_dt" +
    " ,sf.sf_id as sf_id" +
    " ,sf.warehouse_id as warehouse_id" +
    " ,sf.parent_id as parent_id" +
    " ,sf.search_label as search_label" +
    " ,sf.search_label_en as search_label_en" +
    " ,sf.search_label_ru as search_label_ru" +
    " ,sf.search_value as search_value" +
    " ,sf.search_sort as search_sort" +
    " ,sf.filter_action_id as filter_action_id" +
    " ,sf.search_tips as search_tips" +
    " ,sf.search_input_id as search_input_id" +
    " ,sf.sf_desc as sf_desc" +
    " FROM (select * from ods.zy_wms_search_filter where day= "+ nowDate +") as sf"

  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task,tableName,Array(gc_wms,zy_wms))
  }
}
