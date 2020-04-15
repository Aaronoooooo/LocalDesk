package com.zongteng.ztetl.etl.dw.fact.full.orders

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_fact_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DWFactOmsOrdersFull {

  //任务名称(一般同类名)
  private val task = "SPARK_ETL_DW_Fact_Oms_Orders"

  //dw层类名
  private val tableName = "fact_oms_orders"

  // 获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //
  val gc_oms = "select od.row_wid as row_wid\n" +
    "    , od.etl_proc_wid as etl_proc_wid\n" +
    "    , od.w_insert_dt as w_insert_dt\n" +
    "    , od.w_update_dt as w_update_dt\n" +
    "    , od.datasource_num_id as datasource_num_id\n" +
    "    , od.data_flag as data_flag\n" +
    "    , od.integration_id as integration_id\n" +
    "    , od.created_on_dt as created_on_dt\n" +
    "    , od.changed_on_dt as changed_on_dt\n" +
    "    , null as timezone\n" +
    "    , null as exchange_rate\n" +
    "    , com.row_wid as company_key\n" +
    "    , concat(od.datasource_num_id,od.warehouse_id) as warhouse_key\n" +
    "    , sm.row_wid as sm_key\n" +
    "    , od.order_id as order_id\n" +
    "    , od.platform as order_platform\n" +
    "    , od.customer_id as order_customer_id\n" +
    "    , od.company_code as order_company_code\n" +
    "    , od.order_type as order_type\n" +
    "    , nvl(pv1.vl_name, od.order_type) as order_type_val\n" +
    "    , od.create_type as order_create_type\n" +
    "    , nvl(pv2.vl_name, od.create_type) as order_create_type_val\n" +
    "    , od.order_status as order_status\n" +
    "    , nvl(pv3.vl_name, od.order_status) as order_status_val\n" +
    "    , od.sub_status as order_sub_status\n" +
    "    , nvl(pv4.vl_name, od.sub_status) as order_sub_status_val\n" +
    "    , od.cancel_status as order_cancel_status\n" +
    "    , nvl(pv5.vl_name, od.cancel_status) as order_cancel_status_val\n" +
    "    , od.create_method as order_create_method\n" +
    "    , od.shipping_method as order_shipping_method\n" +
    "    , od.shipping_method_platform as order_shipping_method_platform\n" +
    "    , od.warehouse_id as order_warehouse_id\n" +
    "    , od.warehouse_code as order_warehouse_code\n" +
    "    , od.shipping_method_no as order_shipping_method_no\n" +
    "    , od.is_oda as order_is_oda\n" +
    "    , nvl(pv6.vl_name, od.is_oda) as order_is_oda_val\n" +
    "    , od.oda_type as order_oda_type\n" +
    "    , od.is_signature as order_is_signature\n" +
    "    , nvl(pv7.vl_name, od.is_signature) as order_is_signature_val\n" +
    "    , od.is_insurance as order_is_insurance\n" +
    "    , nvl(pv8.vl_name, od.is_insurance) as order_is_insurance_val\n" +
    "    , od.insurance_value as order_insurance_value\n" +
    "    , od.order_weight as order_weight\n" +
    "    , od.order_desc as order_desc\n" +
    "    , od.date_create as order_date_create\n" +
    "    , od.date_release as order_date_release\n" +
    "    , od.date_pickup as order_date_pickup\n" +
    "    , od.date_warehouse_shipping as order_date_warehouse_shipping\n" +
    "    , od.date_last_modify as order_date_last_modify\n" +
    "    , od.refrence_no as order_refrence_no\n" +
    "    , od.refrence_no_platform as order_refrence_no_platform\n" +
    "    , od.refrence_no_sys as order_refrence_no_sys\n" +
    "    , od.refrence_no_warehouse as order_refrence_no_warehouse\n" +
    "    , od.shipping_address_id as order_shipping_address_id\n" +
    "    , od.operator_id as order_operator_id\n" +
    "    , od.operator_note as order_operator_note\n" +
    "    , od.sync_status as order_sync_status\n" +
    "    , od.sync_time as order_sync_time\n" +
    "    , od.date_create_platform as order_date_create_platform\n" +
    "    , od.date_paid_platform as order_date_paid_platform\n" +
    "    , od.date_paid_int as order_date_paid_int\n" +
    "    , od.amountpaid as order_amountpaid\n" +
    "    , od.subtotal as order_subtotal\n" +
    "    , od.ship_fee as order_ship_fee\n" +
    "    , od.platform_fee as order_platform_fee\n" +
    "    , od.finalvaluefee as order_finalvaluefee\n" +
    "    , od.delivery_fee as order_delivery_fee\n" +
    "    , od.currency as order_currency\n" +
    "    , od.user_account as order_user_account\n" +
    "    , od.buyer_id as order_buyer_id\n" +
    "    , od.third_part_ship as order_third_part_ship\n" +
    "    , od.is_merge as order_is_merge\n" +
    "    , od.site as order_site\n" +
    "    , od.abnormal_type as order_abnormal_type\n" +
    "    , nvl(pv9.vl_name, od.abnormal_type) as order_abnormal_type_val\n" +
    "    , od.abnormal_reason as order_abnormal_reason\n" +
    "    , nvl(pv10.vl_name, od.abnormal_reason) as order_abnormal_reason_val\n" +
    "    , od.is_one_piece as order_is_one_piece\n" +
    "    , nvl(pv11.vl_name, od.is_one_piece) as order_is_one_piece_val\n" +
    "    , od.product_count as order_product_count\n" +
    "    , od.consignee_country as order_consignee_country\n" +
    "    , od.buyer_name as order_buyer_name\n" +
    "    , od.buyer_mail as order_buyer_mail\n" +
    "    , od.has_buyer_note as order_has_buyer_note\n" +
    "    , od.fulfillment_channel as order_fulfillment_channel\n" +
    "    , od.ship_service_level as order_ship_service_level\n" +
    "    , od.shipment_service_level_category as order_shipment_service_level_category\n" +
    "    , od.leave_comment as order_leave_comment\n" +
    "    , od.ebay_case_type as order_ebay_case_type\n" +
    "    , od.order_refund as order_refund\n" +
    "    , od.process_again as order_process_again\n" +
    "    , od.has_export as order_has_export\n" +
    "    , od.has_pickup as order_has_pickup\n" +
    "    , od.has_print_pickup_label as order_has_print_pickup_label\n" +
    "    , od.service_status as order_service_status\n" +
    "    , od.service_provider as order_service_provider\n" +
    "    , od.ot_id as order_ot_id\n" +
    "    , od.sys_tips as order_sys_tips\n" +
    "    , od.consignee_name as order_consignee_name\n" +
    "    , od.consignee_company as order_consignee_company\n" +
    "    , od.consignee_street1 as order_consignee_street1\n" +
    "    , od.consignee_street2 as order_consignee_street2\n" +
    "    , od.consignee_street3 as order_consignee_street3\n" +
    "    , od.consignee_district as order_consignee_district\n" +
    "    , od.consignee_county as order_consignee_county\n" +
    "    , od.consignee_city as order_consignee_city\n" +
    "    , od.consignee_state as order_consignee_state\n" +
    "    , od.consignee_country_code as order_consignee_country_code\n" +
    "    , od.consignee_country_name as order_consignee_country_name\n" +
    "    , od.consignee_phone as order_consignee_phone\n" +
    "    , od.consignee_email as order_consignee_email\n" +
    "    , od.consignee_postal_code as order_consignee_postal_code\n" +
    "    , od.consignee_doorplate as order_consignee_doorplate\n" +
    "    , od.shared_sign as order_shared_sign\n" +
    "    , od.is_returns as order_is_returns\n" +
    "    , nvl(pv12.vl_name, od.is_returns) as order_is_returns_val\n" +
    "    , od.is_shipping_method_not_allow_update as order_is_shipping_method_not_allow_update\n" +
    "    , nvl(pv13.vl_name, od.is_shipping_method_not_allow_update) as order_is_shipping_method_not_allow_update_val\n" +
    "    , od.is_fba as order_is_fba\n" +
    "    , nvl(pv14.vl_name, od.is_fba) as order_is_fba_val\n" +
    "    , od.consignee_is_residential as order_consignee_is_residential\n" +
    "    , od.is_more_box as order_is_more_box\n" +
    "    , nvl(pv15.vl_name, od.is_more_box) as order_is_more_box_val\n" +
    "    , od.is_attachment as order_is_attachment\n" +
    "    , nvl(pv16.vl_name, od.is_attachment) as order_is_attachment_val\n" +
    "    , od.so_length as order_so_length\n" +
    "    , od.o_timestamp as order_o_timestamp\n" +
    "    , od.so_width as order_so_width\n" +
    "    , od.so_height as order_so_height\n" +
    "    , od.age_detection as order_age_detection\n" +
    "    , od.payment_time as order_payment_time\n" +
    "    , od.is_recommend as order_is_recommend\n" +
    "    , nvl(pv17.vl_name, od.is_recommend) as order_is_recommend_val\n" +
    "    , od.is_truck_service as order_is_truck_service\n" +
    "    , nvl(pv18.vl_name, od.is_truck_service) as order_is_truck_service_val\n" +
    "    , od.new_order_type as order_new_order_type\n" +
    "    , nvl(pv19.vl_name, od.new_order_type) as order_new_order_type_val\n" +
    "    , od.design_batch_status as order_design_batch_status\n" +
    "    , nvl(pv20.vl_name, od.design_batch_status) as order_design_batch_status_val\n" +
    "    , date_format(od.date_create, 'yyyyMM') as month\n" +
    "from (select * from ods.gc_oms_orders where data_flag<>'delete' ) od\n" +
    s"left join ${Dw_dim_common.getDimSql("gc_oms_company","com")} on com.company_code=od.company_code\n" +
    s"left join ${Dw_dim_common.getDimSql("gc_oms_shipping_method","sm")} on sm.sm_code=od.shipping_method\n" +
    "left join dw.par_val_list as pv1 on pv1.row_wid=concat(od.datasource_num_id,'_', 'order_type','_', od.order_type,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv2 on pv2.row_wid=concat(od.datasource_num_id,'_', 'order_create_type','_', od.create_type,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv3 on pv3.row_wid=concat(od.datasource_num_id,'_', 'order_status','_', od.order_status,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv4 on pv4.row_wid=concat(od.datasource_num_id,'_', 'sub_status','_', od.sub_status,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv5 on pv5.row_wid=concat(od.datasource_num_id,'_', 'cancel_status','_', od.cancel_status,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv6 on pv6.row_wid=concat(od.datasource_num_id,'_', 'is_oda','_', od.is_oda,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv7 on pv7.row_wid=concat(od.datasource_num_id,'_', 'is_signature','_', od.is_signature,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv8 on pv8.row_wid=concat(od.datasource_num_id,'_', 'is_insurance','_', od.is_insurance,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv9 on pv9.row_wid=concat(od.datasource_num_id,'_', 'abnormal_type','_', od.abnormal_type,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv10 on pv10.row_wid=concat(od.datasource_num_id,'_', 'abnormal_reason','_', od.abnormal_reason,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv11 on pv11.row_wid=concat(od.datasource_num_id,'_', 'is_one_piece','_', od.is_one_piece,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv12 on pv12.row_wid=concat(od.datasource_num_id,'_', 'is_returns','_', od.is_returns,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv13 on pv13.row_wid=concat(od.datasource_num_id,'_', 'is_shipping_method_not_allow_update','_', od.is_shipping_method_not_allow_update,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv14 on pv14.row_wid=concat(od.datasource_num_id,'_', 'is_fba','_', od.is_fba,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv15 on pv15.row_wid=concat(od.datasource_num_id,'_', 'is_more_box','_', od.is_more_box,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv16 on pv16.row_wid=concat(od.datasource_num_id,'_', 'is_attachment','_', od.is_attachment,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv17 on pv17.row_wid=concat(od.datasource_num_id,'_', 'is_recommend','_', od.is_recommend,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv18 on pv18.row_wid=concat(od.datasource_num_id,'_', 'is_truck_service','_', od.is_truck_service,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv19 on pv19.row_wid=concat(od.datasource_num_id,'_', 'new_order_type','_', od.new_order_type,'_', 'gc_oms_orders')\n" +
    "left join dw.par_val_list as pv20 on pv20.row_wid=concat(od.datasource_num_id,'_', 'design_batch_status','_', od.design_batch_status,'_', 'gc_oms_orders')"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_oms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_fact_common.getRunCode_hive_full_Into(task, tableName, sqlArray, Array(SystemCodeUtil.GC_OMS))
  }

}
