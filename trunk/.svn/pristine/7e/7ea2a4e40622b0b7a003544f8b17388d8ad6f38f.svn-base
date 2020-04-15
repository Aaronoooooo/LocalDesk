package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimTcmsServer {
  //任务名称(一般同类名)
  private val task = "DimTcmsServer"

  //dw层类名
  private val tableName = "dim_tcms_server"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
 private val gc_tcms="SELECT csp.row_wid as row_wid" +
   " ,from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid" +
   " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
   " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
   " ,csp.datasource_num_id as datasource_num_id" +
   " ,csp.data_flag as data_flag" +
   " ,csp.integration_id as integration_id" +
   " ,csp.created_on_dt as created_on_dt" +
   " ,csp.changed_on_dt as changed_on_dt" +
   " ,cast(concat(csp.datasource_num_id,csp.og_id) as bigint) as organization_key" +
   " ,csp.server_id as server_id" +
   " ,csp.server_code as server_code" +
   " ,csp.server_shortname as server_shortname" +
   " ,csp.server_allname as server_allname" +
   " ,csp.server_statuscode as server_statuscode" +
   " ,nvl(pvl1.vl_bi_name,csp.server_statuscode) as server_statuscode_val" +
   " ,csp.server_levelcode as server_levelcode" +
   " ,csp.server_typecode as server_typecode" +
   " ,csp.settlement_typescode as server_settlement_typescode" +
   " ,nvl(pvl2.vl_bi_name,csp.settlement_typescode) as server_settlement_typescode_val" +
   " ,csp.server_createdate as server_createdate" +
   " ,csp.server_createrid as server_createrid" +
   " ,csp.og_id as server_og_id" +
   " ,csp.tms_id as server_tms_id" +
   " ,csp.manifast_id as server_manifast_id" +
   " ,csp.bag_rule as server_bag_rule" +
   " ,csp.bag_custom_lable_id as server_bag_custom_lable_id" +
   " ,csp.last_update_time as server_last_update_time" +
   " FROM (select * from ods.gc_tcms_csi_server where day= "+ nowDate +" ) as csp" +
   " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='server_statuscode' and pvl.vl_datasource_table='gc_tcms_csi_server') as pvl1 on pvl1.vl_value=csp.server_statuscode" +
   " left join  (select * from dw.par_val_list as pvl where pvl.datasource_num_id='9020' and pvl.vl_type='settlement_typescode' and pvl.vl_datasource_table='gc_tcms_csi_server') as pvl2 on pvl2.vl_value=csp.settlement_typescode"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_tcms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task, tableName, sqlArray, Array(SystemCodeUtil.GC_TCMS))
  }
}
