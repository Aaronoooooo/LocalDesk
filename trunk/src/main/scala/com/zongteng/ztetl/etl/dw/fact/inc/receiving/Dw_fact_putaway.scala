package com.zongteng.ztetl.etl.dw.fact.inc.receiving

import com.zongteng.ztetl.common.{Dw_fact_common, JdbcWithTimeStart}
import com.zongteng.ztetl.util.DateUtil

object Dw_fact_putaway {

  JdbcWithTimeStart.getstart_mysql()
  //任务名称(一般同类名)
  private val task = "Dw_fact_putaway_full"

  //dw层类名
  private val tableName = "fact_putaway"

  // 获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()
  //要执行的sql语句
  private val gc_wms = "SELECT pa.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,pa.datasource_num_id as datasource_num_id" +
    " ,pa.data_flag as data_flag" +
    " ,pa.putaway_id as integration_id" +
    " ,pa.created_on_dt as created_on_dt" +
    " ,pa.changed_on_dt as changed_on_dt" +
    " ,case when (ptz.timezone_season_type='summer_time' and pa.putaway_add_time between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and pa.putaway_add_time not between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_summer_number" +
    " when  (ptz.timezone_season_type='summer_time' and pa.putaway_add_time not between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and pa.putaway_add_time  between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_winner_number" +
    " else null end " +
    " as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(pa.datasource_num_id,pa.warehouse_id) as bigint) as warehouse_key" +
    " ,pa.putaway_id as putaway_id" +
    " ,pa.putaway_code as putaway_code" +
    " ,pa.receiving_code as putaway_receiving_code" +
    " ,pa.warehouse_id as putaway_warehouse_id" +
    " ,wa.warehouse_code as putaway_warehouse_code" +
    " ,pa.putaway_note as putaway_note" +
    " ,pa.putaway_add_time as putaway_add_time" +
    " ,pa.putaway_update_time as putaway_update_time" +
    " ,pa.transit_receiving_code as putaway_transit_receiving_code" +
    " ,pa.pa_timestamp as putaway_pa_timestamp" +
    " ,date_format(pa.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.gc_wms_putaway where data_flag<>'delete' ) as pa" +
    " left join (select * from ods.gc_wms_warehouse where day= "+ nowDate +" ) as wa on wa.warehouse_id=pa.warehouse_id" +
    " left join dw.par_timezone as ptz on ptz.warehouse_code=wa.warehouse_code and date_format(pa.putaway_add_time,'yyyy')=ptz.timezone_year"
  private val zy_wms = "SELECT pa.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,pa.datasource_num_id as datasource_num_id" +
    " ,pa.data_flag as data_flag" +
    " ,pa.putaway_id as integration_id" +
    " ,pa.created_on_dt as created_on_dt" +
    " ,pa.changed_on_dt as changed_on_dt" +
    " ,case when (ptz.timezone_season_type='summer_time' and pa.putaway_add_time between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and pa.putaway_add_time not between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_summer_number" +
    " when  (ptz.timezone_season_type='summer_time' and pa.putaway_add_time not between ptz.timezone_season_start and ptz.timezone_season_end)" +
    " or" +
    " (ptz.timezone_season_type='winner_time' and pa.putaway_add_time  between ptz.timezone_season_start and ptz.timezone_season_end) " +
    " then ptz.timezone_winner_number" +
    " else null end " +
    " as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(pa.datasource_num_id,pa.warehouse_id) as bigint) as warehouse_key" +
    " ,pa.putaway_id as putaway_id" +
    " ,pa.putaway_code as putaway_code" +
    " ,pa.receiving_code as putaway_receiving_code" +
    " ,pa.warehouse_id as putaway_warehouse_id" +
    " ,wa.warehouse_code as putaway_warehouse_code" +
    " ,pa.putaway_note as putaway_note" +
    " ,pa.putaway_add_time as putaway_add_time" +
    " ,pa.putaway_update_time as putaway_update_time" +
    " ,pa.transit_receiving_code as putaway_transit_receiving_code" +
    " ,pa.pa_timestamp as putaway_pa_timestamp" +
    " ,date_format(pa.created_on_dt,'yyyyMM') as month" +
    " FROM (select * from ods.zy_wms_putaway where data_flag<>'delete' ) as pa " +
    " left join (select * from ods.zy_wms_warehouse where day= "+ nowDate +" ) as wa on wa.warehouse_id=pa.warehouse_id" +
    " left join dw.par_timezone as ptz on ptz.warehouse_code=wa.warehouse_code and date_format(pa.putaway_add_time,'yyyy')=ptz.timezone_year"

  def main(args: Array[String]): Unit = {
//传入一个参数,代表往前多少天的增量
    val beforeDate: Int = args(0).toInt
    Dw_fact_common.getRunCode_Hive_inc_info(task, tableName, Array(gc_wms,zy_wms))
  }
}
