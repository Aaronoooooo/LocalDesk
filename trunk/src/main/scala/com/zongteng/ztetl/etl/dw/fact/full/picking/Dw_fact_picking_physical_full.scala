package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.Dw_fact_common

/**
  * 拣货单对应物理仓表
  */
object Dw_fact_picking_physical_full {

  //任务名称(一般同类名)
  private val task = "Dw_fact_picking_physical_full"

  //dw层类名
  private val tableName = "fact_picking_physical"

  //头表名称
  private  val headTable="picking"
  //尾表名称
  private  val endTable="picking_physical_relation"

  //一共那些系统
  private  val odsTableHeads=Array(
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
    "gc_owms_uswe")

  private val sql="SELECT ppr.row_wid as row_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " ,ppr.datasource_num_id as datasource_num_id" +
    " ,ppr.data_flag as data_flag" +
    " ,ppr.integration_id as integration_id" +
    " ,ppr.created_on_dt as created_on_dt" +
    " ,ppr.changed_on_dt as changed_on_dt" +
    " ,null as timezone" +
    " ,null as exchange_rate" +
    " ,cast(concat(ppr.datasource_num_id,pk.warehouse_id) as bigint) as warehouse_key" +
    " ,ppr.ppr_id as ppr_id" +
    " ,ppr.wp_code as ppr_wp_code" +
    " ,pk.picking_id as ppr_picking_id" +
    " ,ppr.picking_code as ppr_picking_code" +
    " ,ppr.lc_code as ppr_lc_code" +
    " FROM (select * from  ods.GcOwmsPickingPhysicalRelationEndTable where data_flag<>'delete') as ppr " +
    " left join (select * from ods.GcOwmsPickingHeadTable where data_flag<>'delete') as pk on ppr.picking_code=pk.picking_code"

  def makeSelectSql()={
    odsTableHeads.map((str:String)=>{
      sql.replace("GcOwmsPickingPhysicalRelationEndTable",str.concat("_").concat(endTable))
        .replace("GcOwmsPickingHeadTable",str.concat("_").concat(headTable))
    })
  }

  def main(args: Array[String]): Unit = {
    val selectSql: Array[String] = makeSelectSql()
    Dw_fact_common.getRunCode_hive_nopartition_full_Into(task,tableName,selectSql)
  }
}
