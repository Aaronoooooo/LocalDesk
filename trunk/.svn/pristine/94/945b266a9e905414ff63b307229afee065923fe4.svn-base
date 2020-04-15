package com.zongteng.ztetl.etl.dw.fact.full.picking

import com.zongteng.ztetl.common.{Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DW_fact_advance_picking_detail_full {

    //任务名称(一般同类名)
    private val task = "DW_fact_advance_picking_detail_full"

    //dw层类名
    private val tableName = "fact_advance_picking_detail"

    // 获取当天的时间
    private val nowDate: String = DateUtil.getNowTime()

    def getGcOwmsSql = {
        //要执行的sql语句
         val select_gc_owms_advance_picking_detail =
        "select adp.row_wid, " +
          "from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid, " +
          "current_timestamp ( ) as w_insert_dt, " +
          "current_timestamp ( ) as w_update_dt, " +
          "adp.datasource_num_id as datasource_num_id, " +
          "adp.data_flag as data_flag, " +
          "adp.integration_id as integration_id, " +
          "adp.created_on_dt as created_on_dt, " +
          "adp.changed_on_dt as changed_on_dt, " +
          "null AS timezone , " +
          "null AS exchange_rate, " +
          "CONCAT(adp.datasource_num_id, adp.warehouse_id)  as warehouse_key, " +
          "CONCAT(adp.datasource_num_id, adp.product_id)  as product_key, " +
          "adp.apd_id, " +
          "adp.warehouse_id, " +
          "adp.order_id, " +
          "adp.order_code, " +
          "adp.pd_status, " +
          "adp.pc_id, " +
          "adp.op_id, " +
          "adp.product_id, " +
          "adp.product_barcode, " +
          "adp.pd_quantity, " +
          "adp.ibo_id, " +
          "adp.ib_id, " +
          "adp.pv_size, " +
          "adp.pick_sort, " +
          "adp.pick_point, " +
          "adp.wa_code, " +
          "adp.lc_code, " +
          "adp.receiving_id, " +
          "adp.receiving_code, " +
          "adp.po_code, " +
          "adp.pd_add_time, " +
          "adp.is_flow_volume, " +
          "adp.wp_code, " +
          "nvl(pv11.vl_bi_name,adp.pd_status), " +
          "nvl(pv12.vl_bi_name,adp.is_flow_volume) " +
          "from " +
          "(select * from ods.gc_owms_advance_picking_detail where data_flag <> 'delete') as adp " +
          "left join " +
          "(select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and  pvl.vl_type='pd_status' and pvl.vl_datasource_table='gc_wms_advance_picking_detail')as pv11 " +
          "on pv11.vl_value = adp.pd_status  " +
          "left join " +
          "(select * from dw.par_val_list as pvl where pvl.datasource_num_id='9004' and  pvl.vl_type='is_flow_volume' and pvl.vl_datasource_table='gc_wms_advance_picking_detail')as pv12 " +
          "on pv12.vl_value = adp.is_flow_volume"

        // 14个表
        val odsTable = Array(
            "gc_owms_usea_advance_picking_detail",
            "gc_owms_uswe_advance_picking_detail",
            "gc_owms_au_advance_picking_detail",
            "gc_owms_cz_advance_picking_detail",
            "gc_owms_de_advance_picking_detail",
            "gc_owms_es_advance_picking_detail",
            "gc_owms_frvi_advance_picking_detail",
            "gc_owms_it_advance_picking_detail",
            "gc_owms_jp_advance_picking_detail",
            "gc_owms_uk_advance_picking_detail",
            "gc_owms_ukob_advance_picking_detail",
            "gc_owms_usnb_advance_picking_detail",
            "gc_owms_usot_advance_picking_detail",
            "gc_owms_ussc_advance_picking_detail"
        )

        odsTable.map(select_gc_owms_advance_picking_detail.replace("gc_owms_advance_picking_detail", _))
    }

    def getZyOwmsSql = {
         val select_zy_owms_advance_picking_detail =
        "select adp.row_wid, " +
          "from_unixtime(unix_timestamp(),'yyyyMMdd') as etl_proc_wid, " +
          "current_timestamp ( ) as w_insert_dt, " +
          "current_timestamp ( ) as w_update_dt, " +
          "adp.datasource_num_id as datasource_num_id, " +
          "adp.data_flag as data_flag, " +
          "adp.integration_id as integration_id, " +
          "adp.created_on_dt as created_on_dt, " +
          "adp.changed_on_dt as changed_on_dt, " +
          "null AS timezone , " +
          "null AS exchange_rate, " +
          "CONCAT(adp.datasource_num_id, adp.warehouse_id)  as warehouse_key, " +
          "CONCAT(adp.datasource_num_id, adp.product_id)  as product_key, " +
          "adp.apd_id, " +
          "adp.warehouse_id, " +
          "adp.order_id, " +
          "adp.order_code, " +
          "adp.pd_status, " +
          "adp.pc_id, " +
          "adp.op_id, " +
          "adp.product_id, " +
          "adp.product_barcode, " +
          "adp.pd_quantity, " +
          "adp.ibo_id, " +
          "adp.ib_id, " +
          "adp.pv_size, " +
          "adp.pick_sort, " +
          "adp.pick_point, " +
          "adp.wa_code, " +
          "adp.lc_code, " +
          "adp.receiving_id, " +
          "adp.receiving_code, " +
          "adp.po_code, " +
          "adp.pd_add_time, " +
          "null as is_flow_volume, " +
          "null as wp_code, " +
          "null as pd_status_val, " +
          "null as is_flow_volume_val " +
          "from " +
          "(select * from ods.zy_owms_advance_picking_detail where data_flag <> 'delete') as adp "

        val odsTables = Array(
            "zy_owms_au_advance_picking_detail",
            "zy_owms_cz_advance_picking_detail",
            "zy_owms_de_advance_picking_detail",
            "zy_owms_ru_advance_picking_detail",
            "zy_owms_uk_advance_picking_detail",
            "zy_owms_usea_advance_picking_detail",
            "zy_owms_uswe_advance_picking_detail",
            "zy_owms_ussc_advance_picking_detail"
        )

        odsTables.map(select_zy_owms_advance_picking_detail.replace("zy_owms_advance_picking_detail", _))
    }


    private val gc_zy_arrary: Array[String] = Array(
        SystemCodeUtil.GC_OWMS_USEA,
        SystemCodeUtil.GC_OWMS_USWE,
        SystemCodeUtil.GC_OWMS_AU,
        SystemCodeUtil.GC_OWMS_CZ,
        SystemCodeUtil.GC_OWMS_DE,
        SystemCodeUtil.GC_OWMS_ES,
        SystemCodeUtil.GC_OWMS_FRVI,
        SystemCodeUtil.GC_OWMS_IT,
        SystemCodeUtil.GC_OWMS_JP,
        SystemCodeUtil.GC_OWMS_UK,
        SystemCodeUtil.GC_OWMS_UKOB,
        SystemCodeUtil.GC_OWMS_USNB,
        SystemCodeUtil.GC_OWMS_USOT,
        SystemCodeUtil.GC_OWMS_USSC,
        SystemCodeUtil.ZY_OWMS_AU,
        SystemCodeUtil.ZY_OWMS_CZ,
        SystemCodeUtil.ZY_OWMS_DE,
        SystemCodeUtil.ZY_OWMS_RU,
        SystemCodeUtil.ZY_OWMS_UK,
        SystemCodeUtil.ZY_OWMS_USEA,
        SystemCodeUtil.ZY_OWMS_USWE,
        SystemCodeUtil.ZY_OWMS_USSC
    )
    def main(args: Array[String]): Unit = {

         val sqlArray: Array[String] = getGcOwmsSql.union(getZyOwmsSql).
           map(_.replaceAll("dw.par_val_list",Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
        Dw_fact_common.getRunCode_hive_nopartition_full_Into(task,tableName, sqlArray,gc_zy_arrary)
    }
}
