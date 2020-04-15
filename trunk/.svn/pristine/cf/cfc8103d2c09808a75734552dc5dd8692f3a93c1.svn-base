package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimWarehouseLocation {
  //任务名称(一般同类名)
  private val task = "Dw_dim_warehouse_location"

  //dw层类名
  private val tableName = "dim_warehouse_location"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms = "SELECT lc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,lc.datasource_num_id as datasource_num_id" +
    " ,lc.data_flag as data_flag" +
    " ,lc.integration_id as integration_id" +
    " ,lc.created_on_dt as  created_on_dt" +
    " ,lc.changed_on_dt as changed_on_dt" +
    " ,cast(concat(lc.datasource_num_id,lc.warehouse_id) as bigint) as warehouse_key" +
    " ,wa.row_wid as wa_key" +
    " ,lc.lc_id as wlc_id" +
    " ,lc.lc_code as wlc_code" +
    " ,lc.lc_note as wlc_note" +
    " ,lc.lc_status as wlc_status" +
    " ,nvl(pvl4.vl_bi_name,lc.lc_status) as wlc_status_val" +
    " ,lc.warehouse_id as wlc_warehouse_id" +
    " ,wh.warehouse_code as wlc_warehouse_code" +
    " ,lc.wa_code as wlc_wa_code" +
    " ,lct.lt_id as wlc_lt_id" +
    " ,lc.lt_code as wlc_lt_code" +
    " ,lct.lt_description as wlc_lt_description" +
    " ,lct.lt_width as wlc_lt_width" +
    " ,lct.lt_length as wlc_lt_length" +
    " ,lct.lt_height as wlc_lt_height" +
    " ,lct.lt_vol as wlc_lt_vol" +
    " ,lc.lc_lock as wlc_lock" +
    " ,lc.lc_sort as wlc_sort" +
    " ,lc.pick_point as wlc_pick_point" +
    " ,lc.pick_sort as wlc_pick_sort" +
    " ,lc.contain_battery as wlc_contain_battery" +
    " ,nvl(pvl1.vl_bi_name,lc.contain_battery) as wlc_contain_battery_val" +
    " ,lc.putaway_sort as wlc_putaway_sort" +
    " ,lc.is_mixed as wlc_is_mixed" +
    " ,nvl(pvl2.vl_bi_name,lc.is_mixed) as wlc_is_mixed_val" +
    " ,lc.is_multiple_batch as wlc_is_multiple_batch" +
    " ,nvl(pvl3.vl_bi_name,lc.is_multiple_batch) as wlc_is_multiple_batch_val" +
    " ,lc.max_weight as wlc_max_weight" +
    " ,lc.max_sku_type as wlc_max_sku_type" +
    " ,lc.lc_add_time as wlc_add_time" +
    " ,lc.lc_update_time as wlc_update_time" +
    " FROM (select * from ods.gc_wms_location where day= "+ nowDate +" ) as lc" +
    " LEFT JOIN (" +
    "     select lct.lt_code, lct.warehouse_id, max(lct.lt_id) as max_lt_id" +
    "     from ods.gc_wms_location_type as lct where lct.day= "+ nowDate +" " +
    "     group by lct.lt_code, lct.warehouse_id" +
    " ) as ll on ll.warehouse_id=lc.warehouse_id and ll.lt_code=lc.lt_code" +
    " left join (select * from ods.gc_wms_location_type where day= "+ nowDate +" ) as lct on lct.lt_id = ll.max_lt_id" +
    " left join (select * from ods.gc_wms_warehouse where day= "+ nowDate +"  ) as wh on wh.warehouse_id=lc.warehouse_id" +
    " left join (select max(dwa.wa_id) as wa_id,dwa.wa_code,max(dwa.row_wid) as row_wid from ods.gc_wms_warehouse_area dwa where dwa.day= "+ nowDate +"  group by dwa.wa_code) as wa on wa.wa_code=lc.wa_code" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='contain_battery' and pvl.vl_datasource_table='gc_wms_location') as pvl1 on pvl1.vl_value=lc.contain_battery" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='is_mixed' and pvl.vl_datasource_table='gc_wms_location') as pvl2 on pvl2.vl_value=lc.is_mixed" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='is_multiple_batch' and pvl.vl_datasource_table='gc_wms_location') as pvl3 on pvl3.vl_value=lc.is_multiple_batch" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='lc_status' and pvl.vl_datasource_table='gc_wms_location') as pvl4 on pvl4.vl_value=lc.lc_status"

  private  val zy_wms="SELECT lc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,lc.datasource_num_id as datasource_num_id" +
    " ,lc.data_flag as data_flag" +
    " ,lc.integration_id as integration_id" +
    " ,lc.created_on_dt as  created_on_dt" +
    " ,lc.changed_on_dt as changed_on_dt" +
    " ,cast(concat(lc.datasource_num_id,lc.warehouse_id) as bigint) as warehouse_key" +
    " ,wa.row_wid as wa_key" +
    " ,lc.lc_id as wlc_id" +
    " ,lc.lc_code as wlc_code" +
    " ,lc.lc_note as wlc_note" +
    " ,lc.lc_status as wlc_status" +
    " ,nvl(pvl4.vl_bi_name,lc.lc_status) as wlc_status_val" +
    " ,lc.warehouse_id as wlc_warehouse_id" +
    " ,wh.warehouse_code as wlc_warehouse_code" +
    " ,lc.wa_code as wlc_wa_code" +
    " ,lct.lt_id as wlc_lt_id" +
    " ,lc.lt_code as wlc_lt_code" +
    " ,lct.lt_description as wlc_lt_description" +
    " ,lct.lt_width as wlc_lt_width" +
    " ,lct.lt_length as wlc_lt_length" +
    " ,lct.lt_height as wlc_lt_height" +
    " ,lct.lt_vol as wlc_lt_vol" +
    " ,lc.lc_lock as wlc_lock" +
    " ,lc.lc_sort as wlc_sort" +
    " ,lc.pick_point as wlc_pick_point" +
    " ,lc.pick_sort as wlc_pick_sort" +
    " ,null as wlc_contain_battery" +
    " ,null as wlc_contain_battery_val" +
    " ,null as wlc_putaway_sort" +
    " ,null as wlc_is_mixed" +
    " ,null as wlc_is_mixed_val" +
    " ,null as wlc_is_multiple_batch" +
    " ,null as wlc_is_multiple_batch_val" +
    " ,null as wlc_max_weight" +
    " ,null as wlc_max_sku_type" +
    " ,lc.lc_add_time as wlc_add_time" +
    " ,lc.lc_update_time as wlc_update_time" +
    " FROM (select * from ods.zy_wms_location where day= "+ nowDate +"  ) as lc" +
    " LEFT JOIN (" +
    "     select lct.lt_code, lct.warehouse_id, max(lct.lt_id) as max_lt_id" +
    "     from ods.zy_wms_location_type as lct where lct.day= "+ nowDate +" " +
    "     group by lct.lt_code, lct.warehouse_id" +
    " ) as ll on ll.warehouse_id=lc.warehouse_id and ll.lt_code=lc.lt_code" +
    " left join (select * from ods.zy_wms_location_type where day= "+ nowDate +"  ) as lct on lct.lt_id = ll.max_lt_id" +
    " left join (select * from ods.zy_wms_warehouse where day= "+ nowDate +"  ) as wh on wh.warehouse_id=lc.warehouse_id" +
    " left join (select max(dwa.wa_id) as wa_id,dwa.wa_code,max(dwa.row_wid) as row_wid from ods.zy_wms_warehouse_area dwa where dwa.day= "+ nowDate +"   group by dwa.wa_code) as wa on wa.wa_code=lc.wa_code" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='lc_status' and pvl.vl_datasource_table='zy_wms_location') as pvl4 on pvl4.vl_value=lc.lc_status"


  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms,zy_wms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task,tableName,sqlArray,Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))

  }

}
