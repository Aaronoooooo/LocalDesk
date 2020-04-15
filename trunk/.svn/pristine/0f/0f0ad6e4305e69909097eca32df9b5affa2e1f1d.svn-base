package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

import scala.collection.{immutable, mutable}

object DimWarehouseLocation {
  //任务名称(一般同类名)
  private val task = "Dw_dim_warehouse_location"

  //dw层类名
  private val tableName = "dim_warehouse_location"

  //维度表名称
  private val locationCommon = "location"
  private val locationTypeCommon = "location_type"
  private val warehouseAreaCommon = "warehouse_area"

  //获取当天时间
  private val nowDate: String = DateUtil.getNowTime()

  //一共那些系统
  private val gcOwmsOdsTableHeads = Map[String, Int]("gc_owms_au"->7,
    "gc_owms_cz"->12,
    "gc_owms_de"->10,
    "gc_owms_es"->21,
    "gc_owms_it"->20,
    "gc_owms_jp"->9,
    "gc_owms_uk"->8,
    "gc_owms_frvi"->18,
    "gc_owms_ukob"->13,
    "gc_owms_usnb"->14,
    "gc_owms_usot"->15,
    "gc_owms_ussc"->23,
    "gc_owms_usea"->1,
    "gc_owms_uswe"->4)
  private val zyOwmsOdsTableHeads = Map[String, Int](
    "zy_owms_au"->7,
    "zy_owms_cz"->13,
    "zy_owms_de"->10,
    "zy_owms_ru"->16,
    "zy_owms_uk"->8,
    "zy_owms_usea"->1,
    "zy_owms_uswe"->4,
    "zy_owms_ussc"->17)

  //要执行的sql语句
  private val gc_owms = "SELECT lc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,'9004' as datasource_num_id" +
    " ,lc.data_flag as data_flag" +
    " ,lc.integration_id as integration_id" +
    " ,lc.created_on_dt as  created_on_dt" +
    " ,lc.changed_on_dt as changed_on_dt" +
    " ,concat('9004',lc.warehouse_id) as warehouse_key" +
    " ,concat('9004',wa.wa_id) as wa_key" +
    " ,wp.row_wid as wp_key" +
    " ,lc.lc_id as wlc_id" +
    " ,lc.lc_code as wlc_code" +
    " ,lc.lc_note as wlc_note" +
    " ,lc.lc_status as wlc_status" +
    " ,nvl(pvl1.vl_bi_name,lc.lc_status) as wlc_status_val" +
    " ,lc.warehouse_id as wlc_warehouse_id" +
    " ,lc.wp_code as wlc_wp_code" +
    " ,lct.lt_id as wlc_lt_id" +
    " ,lc.lt_code as wlc_lt_code" +
    " ,lc.lc_lock as wlc_lock" +
    " ,lc.wa_code as wlc_wa_code" +
    " ,lc.lc_sort as wlc_sort" +
    " ,lc.lc_classify as wlc_classify" +
    " ,nvl(pvl2.vl_bi_name,lc.lc_classify) as wlc_classify_val" +
    " ,lc.lc_level as wlc_level" +
    " ,nvl(pvl3.vl_bi_name,lc.lc_level) as wlc_level_val" +
    " ,lc.pick_point as wlc_pick_point" +
    " ,lc.pick_sort as wlc_pick_sort" +
    " ,lc.lc_add_time as wlc_add_time" +
    " ,lc.lc_update_time as wlc_update_time" +
    " ,lc.contain_battery as wlc_contain_battery" +
    " ,nvl(pvl4.vl_bi_name,lc.contain_battery) as wlc_contain_battery_val" +
    " ,lc.putaway_sort as wlc_putaway_sort" +
    " ,lc.is_mixed as wlc_is_mixed" +
    " ,nvl(pvl5.vl_bi_name,lc.is_mixed) as wlc_is_mixed_val" +
    " ,lc.is_multiple_batch as wlc_is_multiple_batch" +
    " ,nvl(pvl6.vl_bi_name,lc.is_multiple_batch) as wlc_is_multiple_batch_val" +
    " ,lc.max_weight as wlc_max_weight" +
    " ,lc.max_sku_type as wlc_max_sku_type" +
    " ,lc.roadway as wlc_roadway" +
    " ,lc.row_col as wlc_row_col" +
    " ,lc.column_col as wlc_column_col" +
    " ,lc.tier as wlc_tier" +
    " ,lc.direction_indicator as wlc_direction_indicator" +
    " ,nvl(pvl7.vl_bi_name,lc.direction_indicator) as wlc_direction_indicator_val" +
    " ,lc.roadway_add_time as wlc_roadway_add_time" +
    " ,lc.timestamp as wlc_timestamp" +
    " ,lct.lt_description as wlc_lt_description" +
    " ,lct.lt_width as wlc_lt_width" +
    " ,lct.lt_length as wlc_lt_length" +
    " ,lct.lt_height as wlc_lt_height" +
    " ,lct.lt_vol as wlc_lt_vol" +
    s" FROM (select * from ods.OwmsLocationCommon where warehouse_id=WarehouseId  and  day= ${nowDate} ) as lc" +
    " LEFT JOIN (" +
    "     select lct.lt_code, lct.warehouse_id, max(lct.lt_id) as max_lt_id" +
    s"     from ods.OwmsLocationTypeCommon as lct where lct.day= ${nowDate}" +
    "     group by lct.lt_code, lct.warehouse_id" +
    " ) as ll on ll.warehouse_id=lc.warehouse_id and ll.lt_code=lc.lt_code" +
    s" left join (select * from ods.OwmsLocationTypeCommon where day= ${nowDate}) as lct on lct.lt_id = ll.max_lt_id" +
    s" left join (select max(dwa.wa_id) as wa_id,dwa.wa_code,max(dwa.row_wid) as row_wid from ods.OwmsWarehouseArea dwa where dwa.day= ${nowDate} group by dwa.wa_code) as wa on wa.wa_code=lc.wa_code" +
    s" left join (select * from ods.gc_wms_warehouse_physical  where day= ${nowDate} ) as wp on wp.wp_code=lc.wp_code" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='lc_status' and pvl.vl_datasource_table='gc_owms_location') as pvl1 on pvl1.vl_value=lc.lc_status" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='lc_classify' and pvl.vl_datasource_table='gc_owms_location') as pvl2 on pvl2.vl_value=lc.lc_classify" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='lc_level' and pvl.vl_datasource_table='gc_owms_location') as pvl3 on pvl3.vl_value=lc.lc_level" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='contain_battery' and pvl.vl_datasource_table='gc_owms_location') as pvl4 on pvl4.vl_value=lc.contain_battery" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='is_mixed' and pvl.vl_datasource_table='gc_owms_location') as pvl5 on pvl5.vl_value=lc.is_mixed" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='is_multiple_batch' and pvl.vl_datasource_table='gc_owms_location') as pvl6 on pvl6.vl_value=lc.is_multiple_batch" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and pvl.vl_type='direction_indicator' and pvl.vl_datasource_table='gc_owms_location') as pvl7 on pvl7.vl_value=lc.direction_indicator"

  private  val zy_owms="SELECT lc.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,'9022' as datasource_num_id" +
    " ,lc.data_flag as data_flag" +
    " ,lc.integration_id as integration_id" +
    " ,lc.created_on_dt as  created_on_dt" +
    " ,lc.changed_on_dt as changed_on_dt" +
    " ,concat('9022',lc.warehouse_id) as warehouse_key" +
    " ,concat('9022',wa.wa_id) as wa_key" +
    " ,case" +
    "   when lc.warehouse_id=1 then concat('9022',2)" +
    "   when lc.warehouse_id=4 then concat('9022',8)" +
    "   when lc.warehouse_id=7 then concat('9022',1)" +
    "   when lc.warehouse_id=8 then concat('9022',10)" +
    "   when lc.warehouse_id=13 then concat('9022',13)" +
    "   when lc.warehouse_id=17 then concat('9022',22)" +
    "   else null" +
    " end as wp_key" +
    " ,lc.lc_id as wlc_id" +
    " ,lc.lc_code as wlc_code" +
    " ,lc.lc_note as wlc_note" +
    " ,lc.lc_status as wlc_status" +
    " ,nvl(pvl1.vl_bi_name,lc.lc_status) as wlc_status_val" +
    " ,lc.warehouse_id as wlc_warehouse_id" +
    " ,case" +
    "   when lc.warehouse_id=1 then 'USEA-1'" +
    "   when lc.warehouse_id=4 then 'USWE-3'" +
    "   when lc.warehouse_id=7 then 'AU-2'" +
    "   when lc.warehouse_id=8 then 'UK-1'" +
    "   when lc.warehouse_id=13 then 'CZ-2'" +
    "   when lc.warehouse_id=17 then 'USSC-1'" +
    "   else null" +
    " end as wlc_wp_code" +
    " ,lct.lt_id as wlc_lt_id" +
    " ,lc.lt_code as wlc_lt_code" +
    " ,lc.lc_lock as wlc_lock" +
    " ,lc.wa_code as wlc_wa_code" +
    " ,lc.lc_sort as wlc_sort" +
    " ,null as wlc_classify" +
    " ,null as wlc_classify_val" +
    " ,null as wlc_level" +
    " ,null as wlc_level_val" +
    " ,lc.pick_point as wlc_pick_point" +
    " ,lc.pick_sort as wlc_pick_sort" +
    " ,lc.lc_add_time as wlc_add_time" +
    " ,lc.lc_update_time as wlc_update_time" +
    " ,null as wlc_contain_battery" +
    " ,null as wlc_contain_battery_val" +
    " ,null as wlc_putaway_sort" +
    " ,null as wlc_is_mixed" +
    " ,null as wlc_is_mixed_val" +
    " ,null as wlc_is_multiple_batch" +
    " ,null as wlc_is_multiple_batch_val" +
    " ,null as wlc_max_weight" +
    " ,null as wlc_max_sku_type" +
    " ,null as wlc_roadway" +
    " ,null as wlc_row_col" +
    " ,null as wlc_column_col" +
    " ,null as wlc_tier" +
    " ,null as wlc_direction_indicator" +
    " ,null as wlc_direction_indicator_val" +
    " ,null as wlc_roadway_add_time" +
    " ,null as wlc_timestamp" +
    " ,lct.lt_description as wlc_lt_description" +
    " ,lct.lt_width as wlc_lt_width" +
    " ,lct.lt_length as wlc_lt_length" +
    " ,lct.lt_height as wlc_lt_height" +
    " ,lct.lt_vol as wlc_lt_vol" +
    s" FROM (select * from ods.OwmsLocationCommon where warehouse_id=WarehouseId  and  day= ${nowDate} ) as lc" +
    " LEFT JOIN (" +
    "     select lct.lt_code, lct.warehouse_id, max(lct.lt_id) as max_lt_id" +
    s"     from ods.OwmsLocationTypeCommon as lct where lct.day= ${nowDate}" +
    "     group by lct.lt_code, lct.warehouse_id" +
    " ) as ll on ll.warehouse_id=lc.warehouse_id and ll.lt_code=lc.lt_code" +
    s" left join (select * from ods.OwmsLocationTypeCommon where day= ${nowDate}) as lct on lct.lt_id = ll.max_lt_id" +
    s" left join (select max(dwa.wa_id) as wa_id,dwa.wa_code,max(dwa.row_wid) as row_wid from ods.OwmsWarehouseArea dwa where dwa.day= ${nowDate} group by dwa.wa_code) as wa on wa.wa_code=lc.wa_code" +
    " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9022' and pvl.vl_type='lc_status' and pvl.vl_datasource_table='zy_owms_location') as pvl1 on pvl1.vl_value=lc.lc_status"

  def makeSelectSql(mapStr:Map[String, Int],sqlStr:String)={
    val str: Array[String] = mapStr.map((x: (String, Int)) => {
          sqlStr.replaceAll("OwmsLocationCommon", x._1.concat("_").concat(locationCommon))
            .replaceAll("WarehouseId", x._2.toString)
            .replaceAll("OwmsLocationTypeCommon", x._1.concat("_").concat(locationTypeCommon))
            .replaceAll("OwmsWarehouseArea", x._1.concat("_").concat(warehouseAreaCommon))
        })(collection.breakOut)
    str
  }
  def main(args: Array[String]): Unit = {
    val gcOwmsArr: Array[String] = makeSelectSql(gcOwmsOdsTableHeads,gc_owms)
    val zyOwmsArr: Array[String] = makeSelectSql(zyOwmsOdsTableHeads,zy_owms)
    val sqlArray: Array[String] = gcOwmsArr++:zyOwmsArr.map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task,tableName,sqlArray,Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS))

  }

}
