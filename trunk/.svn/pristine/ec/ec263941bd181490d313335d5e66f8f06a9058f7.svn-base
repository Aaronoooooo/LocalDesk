package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache}
import com.zongteng.ztetl.util.DateUtil

object ParCurrencyRate {
  //任务名称(一般同类名)
  private val task = "Dw_par_currency_rate"

  //dw层类名
  private val tableName = "par_currency_rate"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val sql = "select a.cr_datekey as cr_datekey" +
    " ,a.currency_code as cr_currency_code" +
    " ,a.cr_currency_rate as cr_currency_rate" +
    " ,a.cr_former_currency_rate as cr_former_currency_rate" +
    " ,a.cr_user_id as cr_user_id" +
    " ,from_unixtime(unix_timestamp(a.cr_add_time),'yyyy-MM-dd HH:mm:ss') as cr_add_time" +
    " ,from_unixtime((unix_timestamp(nvl(b.cr_add_time,a.cr_end_time))-1),'yyyy-MM-dd HH:mm:ss') as cr_end_time" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_insert_dt" +
    " ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as w_update_dt" +
    " from (select cl.cr_datekey, nvl(cl.currency_code,'') as currency_code" +
    " ,nvl(clmax.to_currency_rate,cast(1.0000 as decimal(18,4))) as cr_currency_rate" +
    " ,nvl(clmin.currency_rate,cast(1.0000 as decimal(18,4))) as cr_former_currency_rate" +
    " ,nvl(clmax.user_id,0) as cr_user_id" +
    " ,case " +
    " when year(nvl(cast(clmax.cl_add_time as string),'2010-01-01 00:00:00'))<='2010' then cast('1900-01-01 00:00:00' as TIMESTAMP)" +
    " else clmax.cl_add_time" +
    " end as cr_add_time" +
    " ,cast('9999-12-31 23:59:59' as TIMESTAMP) as cr_end_time" +
    " ,row_number() over(partition by cl.currency_code order by clmax.cl_add_time asc) as pv" +
    " from (" +
    " select cast(cl.cl_add_time as date) as cr_datekey" +
    " ,nvl(cy.currency_code,cl.currency_code) as currency_code" +
    " ,max(cl.cl_id) as cl_id_max" +
    " ,min(cl.cl_id) as cl_id_min" +
    " from (select * from ods.gc_wms_currency_log where day=" + nowDate + ") cl " +
    " left join (select * from ods.gc_wms_currency where day=" + nowDate + ") as cy on cy.currency_name=cl.currency_code" +
    " left join (select * from ods.gc_wms_currency where day=" + nowDate + ") as cy1 on cy1.currency_code=cl.currency_code" +
    " where cl.user_id<>29" +
    " group by cast(cl.cl_add_time as date),nvl(cy.currency_code,cl.currency_code)) as cl" +
    " left join (select * from ods.gc_wms_currency_log where day=" + nowDate + ") clmax on clmax.cl_id=cl.cl_id_max" +
    " left join (select * from ods.gc_wms_currency_log where day=" + nowDate + ") clmin on clmin.cl_id=cl.cl_id_min) a" +
    " left join " +
    " (select cl.cr_datekey, nvl(cl.currency_code,'') as currency_code" +
    " ,nvl(clmax.to_currency_rate,cast(1.0000 as decimal(18,4))) as cr_currency_rate" +
    " ,nvl(clmin.currency_rate,cast(1.0000 as decimal(18,4))) as cr_former_currency_rate" +
    " ,nvl(clmax.user_id,0) as cr_user_id" +
    " ,case " +
    " when year(nvl(cast(clmax.cl_add_time as string),'2010-01-01 00:00:00'))<='2010' then cast('1900-01-01 00:00:00' as TIMESTAMP)" +
    " else clmax.cl_add_time" +
    " end as cr_add_time" +
    " ,cast('9999-12-31 23:59:59' as TIMESTAMP) as cr_end_time" +
    " ,row_number() over(partition by cl.currency_code order by clmax.cl_add_time asc) as pv" +
    " from (" +
    " select cast(cl.cl_add_time as date) as cr_datekey" +
    " ,nvl(cy.currency_code,cl.currency_code) as currency_code" +
    " ,max(cl.cl_id) as cl_id_max" +
    " ,min(cl.cl_id) as cl_id_min" +
    " from (select * from ods.gc_wms_currency_log where day=" + nowDate + ") cl " +
    " left join (select * from ods.gc_wms_currency where day=" + nowDate + ") as cy on cy.currency_name=cl.currency_code" +
    " left join (select * from ods.gc_wms_currency where day=" + nowDate + ") as cy1 on cy1.currency_code=cl.currency_code" +
    " where cl.user_id<>29" +
    " group by cast(cl.cl_add_time as date),nvl(cy.currency_code,cl.currency_code)) as cl" +
    " left join (select * from ods.gc_wms_currency_log where day=" + nowDate + ") clmax on clmax.cl_id=cl.cl_id_max" +
    " left join (select * from ods.gc_wms_currency_log where day=" + nowDate + ") clmin on clmin.cl_id=cl.cl_id_min) b on " +
    " a.currency_code=b.currency_code and a.pv=b.pv-1"


  def main(args: Array[String]): Unit = {
    Dw_dim_common.getRunCode_full(task,tableName,Array(sql),Dw_par_val_list_cache.EMPTY_PAR_VAL_LIST)

  }
}
