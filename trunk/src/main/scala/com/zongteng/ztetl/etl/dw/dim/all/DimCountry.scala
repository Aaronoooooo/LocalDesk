package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.Dw_dim_common
import com.zongteng.ztetl.util.DateUtil

object DimCountry {
  //任务名称(一般同类名)
  private val task = "Dw_dim_country"

  //dw层类名
  private val tableName = "dim_country"

  //尾表名称
  private val tableNameCommon = "country"

  //获取当天时间
  private val nowDate: String = DateUtil.getNowTime()

  //一共那些系统
  private val zyWmsAndGcOwmsOdsTableHeads = Array(
    "gc_owms_au",
    "gc_owms_cz",
    "gc_owms_de",
    "gc_owms_es",
    "gc_owms_it",
    "gc_owms_jp",
    "gc_owms_uk",
    "gc_owms_frvi",
    "gc_owms_ukob",
    "gc_owms_usnb",
    "gc_owms_usot",
    "gc_owms_ussc",
    "gc_owms_usea",
    "gc_owms_uswe",
    "zy_wms")
  private val zyOwmsOdsTableHeads = Array(
    "zy_owms_au",
    "zy_owms_cz",
    "zy_owms_de",
    "zy_owms_ru",
    "zy_owms_uk",
    "zy_owms_usea",
    "zy_owms_uswe",
    "zy_owms_ussc")

  private val gc_wms = "SELECT coy.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,coy.datasource_num_id as datasource_num_id" +
    " ,coy.data_flag as data_flag" +
    " ,coy.integration_id as integration_id" +
    " ,coy.created_on_dt as created_on_dt" +
    " ,coy.changed_on_dt as changed_on_dt" +
    " ,coy.country_id as country_id" +
    " ,coy.country_name as country_name" +
    " ,coy.country_name_en as country_name_en" +
    " ,coy.country_local_name as country_local_name" +
    " ,coy.country_alias as country_alias" +
    " ,coy.country_code as country_code" +
    " ,coy.country_sort as country_sort" +
    " ,coy.country_short_name as country_short_name" +
    " ,coy.trade_country as country_trade_country" +
    " ,coy.country_num as country_num" +
    " ,coy.zipcode_substring_length as country_zipcode_substring_length" +
    " ,coy.cellphone_area_code as country_cellphone_area_code" +
    " FROM (select * from ods.gc_wms_country where day= " + nowDate + " ) as coy"
  private val zy_wms_gc_owms = " SELECT coy.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,coy.datasource_num_id as datasource_num_id" +
    " ,coy.data_flag as data_flag" +
    " ,coy.integration_id as integration_id" +
    " ,coy.created_on_dt as created_on_dt" +
    " ,coy.changed_on_dt as changed_on_dt" +
    " ,coy.country_id as country_id" +
    " ,coy.country_name as country_name" +
    " ,coy.country_name_en as country_name_en" +
    " ,coy.country_local_name as country_local_name" +
    " ,coy.country_alias as country_alias" +
    " ,coy.country_code as country_code" +
    " ,coy.country_sort as country_sort" +
    " ,coy.country_short_name as country_short_name" +
    " ,coy.trade_country as country_trade_country" +
    " ,coy.country_num as country_num" +
    " ,coy.zipcode_substring_length as country_zipcode_substring_length" +
    " ,null as country_cellphone_area_code" +
    " FROM (select * from ods.zyWmsGcOwmsCountry where day= " + nowDate + " ) as coy"
  private val zy_owms = " SELECT coy.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,coy.datasource_num_id as datasource_num_id" +
    " ,coy.data_flag as data_flag" +
    " ,coy.integration_id as integration_id" +
    " ,coy.created_on_dt as created_on_dt" +
    " ,coy.changed_on_dt as changed_on_dt" +
    " ,coy.country_id as country_id" +
    " ,coy.country_name as country_name" +
    " ,coy.country_name_en as country_name_en" +
    " ,coy.country_local_name as country_local_name" +
    " ,coy.country_alias as country_alias" +
    " ,coy.country_code as country_code" +
    " ,coy.country_sort as country_sort" +
    " ,coy.country_short_name as country_short_name" +
    " ,coy.trade_country as country_trade_country" +
    " ,null as country_num" +
    " ,null as country_zipcode_substring_length" +
    " ,null as country_cellphone_area_code" +
    " FROM (select * from ods.zyOwmsCountry where day= " + nowDate + " ) as coy"

  def makeSelectSql(arrStr:Array[String],sqlStr:String,replaceStr:String)={
    arrStr.map((str:String)=>{
      sqlStr.replace(replaceStr,str.concat("_").concat(tableNameCommon))
    })
  }


  def main(args: Array[String]): Unit = {
    val selectSql1: Array[String] = makeSelectSql(zyWmsAndGcOwmsOdsTableHeads,zy_wms_gc_owms,"zyWmsGcOwmsCountry")
    val selectSql2: Array[String] = makeSelectSql(zyOwmsOdsTableHeads,zy_owms,"zyOwmsCountry")
    Dw_dim_common.getRunCode_full(task,tableName,gc_wms+:selectSql1++:selectSql2)

  }
}
