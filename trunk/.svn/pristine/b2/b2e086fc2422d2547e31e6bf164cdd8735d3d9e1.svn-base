package com.zongteng.ztetl.etl.dw.dim

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimProduct {

  //任务名称(一般同类名)
  private val task = "Dw_dim_product"

  //dw层类名
  private val tableName = "dim_product"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val zy_wms="select" +
    " prt.row_wid as row_wid," +
    " from_unixtime(unix_timestamp(current_date()), 'yyyyMMdd') as etl_proc_wid, " +
    " current_timestamp() as w_insert_dt, " +
    " current_timestamp() as w_update_dt, " +
    " prt.datasource_num_id as datasource_num_id," +
    " prt.data_flag as data_flag," +
    " prt.product_id as integration_id," +
    " prt.created_on_dt as created_on_dt," +
    " prt.changed_on_dt as changed_on_dt," +
    " cast(CONCAT(prt.datasource_num_id, prt.customer_id) as bigint) as customer_key," +
    " prt.product_id as product_id," +
    " prt.product_sku as product_sku," +
    " prt.product_barcode as product_barcode," +
    " prt.reference_no as product_reference_no," +
    " prt.customer_code as product_customer_code," +
    " prt.customer_id as product_customer_id," +
    " regexp_replace(prt.product_title_en, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as product_title_en," +
    " regexp_replace(prt.product_title, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as product_title," +
    " prt.product_declared_name as product_declared_name ," +
    " prt.product_status as product_status," +
    " nvl(v_product_status.vl_bi_name, prt.product_status) as product_status_val," +
    " prt.sale_status as product_sale_status," +
    " nvl(v_sale_status.vl_bi_name, prt.sale_status) as product_sale_status_val," +
    " prt.product_receive_status as product_receive_status," +
    " nvl(v_product_receive_status.vl_bi_name, prt.product_receive_status)as product_receive_status_val," +
    " prt.hs_code as product_hs_code," +
    " prt.currency_code as product_currency_code," +
    " prt.pu_code as product_pu_code," +
    " prt.product_length as product_length," +
    " prt.product_width as product_width," +
    " prt.product_height as product_height," +
    " prt.product_net_weight as product_net_weight," +
    " prt.product_weight as product_weight," +
    " prt.product_sales_value as product_sales_value," +
    " prt.product_purchase_value as product_purchase_value," +
    " prt.product_declared_value as product_declared_value," +
    " prt.product_is_qc as product_is_qc," +
    " nvl(v_product_is_qc.vl_bi_name, prt.product_is_qc) as product_is_qc_val," +
    " prt.product_barcode_type as product_barcode_type," +
    " nvl(v_product_barcode_type.vl_bi_name, prt.product_barcode_type) as product_barcode_type_val," +
    " prt.product_type as product_type," +
    " nvl(v_product_type.vl_bi_name, prt.product_type) as product_type_val," +
    " prt.pc_id as product_pc_id," +
    " prt.pce_id as product_pce_id," +
    " prt.product_add_time as product_add_time," +
    " prt.product_update_time as product_update_time," +
    " prt.have_asn as product_have_asn," +
    " prt.pd_id as product_pd_id," +
    " prt.contain_battery as product_product_attribute," +
    " nvl(v_contain_battery.vl_bi_name, prt.contain_battery) as product_product_attribute_val," +
    " prt.buyer_id as product_buyer_id," +
    " prt.warning_qty as product_warning_qty," +
    " prt.product_package_type as product_package_type," +
    " nvl(v_product_package_type.vl_bi_name, prt.product_package_type) as product_package_type_val," +
    " prt.shared_sign as product_is_shared_sign," +
    " nvl(v_shared_sign.vl_bi_name, prt.shared_sign) as product_is_shared_sign_val," +
    " prt.shared_unit_price as product_shared_unit_price," +
    " prt.product_describe as product_describe ," +
    " prt.shared_time as product_shared_time," +
    " prt.reason as product_reason," +
    " prt.customs_code as product_customs_code," +
    " prt.product_declared_name_zh as product_declared_name_zh ," +
    " prt.create_type as product_create_type," +
    " prt.is_thirdpart as product_is_thirdpart," +
    " nvl(v_is_thirdpart.vl_bi_name, prt.is_thirdpart) as product_is_thirdpart_val," +
    " prt.thirdpart_platform as product_thirdpart_platform ," +
    " prt.relation_barcode as product_relation_barcode ," +
    " null  as  product_mark_weight," +
    " null  as  product_mark_length," +
    " null  as  product_mark_width," +
    " null  as  product_mark_height," +
    " null  as  product_real_weight," +
    " null  as  product_real_height," +
    " null  as  product_real_width," +
    " null  as  product_real_length," +
    " null  as  product_abnormal_time," +
    " null  as  product_oms_add_time," +
    " null as  product_material," +
    " null as  product_brand," +
    " null as  product_model," +
    " null as  product_function," +
    " null as  product_versions," +
    " null as  product_is_irregular," +
    " null as product_is_irregular_val," +
    " null as  product_irregular_values," +
    " null as  product_company_code," +
    " null as  product_desc," +
    " prt.pce_id as product_category_id" +
    " ,case  " +
    "   when pc0.ig_level=0 then pc0.ig_id" +
    "   when pc1.ig_level=0 then pc1.ig_id" +
    "   when pc2.ig_level=0 then pc2.ig_id" +
    "   when pc3.ig_level=0 then pc3.ig_id" +
    "   else 0" +
    " END as product_category_first_id" +
    " ,case  " +
    "   when pc0.ig_level=0 then pc0.ig_name" +
    "   when pc1.ig_level=0 then pc1.ig_name" +
    "   when pc2.ig_level=0 then pc2.ig_name" +
    "   when pc3.ig_level=0 then pc3.ig_name" +
    "   else ''" +
    " END as product_category_first_id_name" +
    " ,case  " +
    "   when pc0.ig_level=0 then pc0.ig_name_en" +
    "   when pc1.ig_level=0 then pc1.ig_name_en" +
    "   when pc2.ig_level=0 then pc2.ig_name_en" +
    "   when pc3.ig_level=0 then pc3.ig_name_en" +
    "   else ''" +
    " END as product_category_first_id_name_en" +
    " ,case   " +
    "   when pc0.ig_level=1 then pc0.ig_id" +
    "   when pc1.ig_level=1 then pc1.ig_id" +
    "   when pc2.ig_level=1 then pc2.ig_id" +
    "   when pc3.ig_level=1 then pc3.ig_id" +
    "   else 0" +
    " END as product_category_second_id" +
    " ,case   " +
    "   when pc0.ig_level=1 then pc0.ig_name" +
    "   when pc1.ig_level=1 then pc1.ig_name" +
    "   when pc2.ig_level=1 then pc2.ig_name" +
    "   when pc3.ig_level=1 then pc3.ig_name" +
    "   else ''" +
    " END as product_category_second_id_name" +
    " ,case   " +
    "   when pc0.ig_level=1 then pc0.ig_name_en" +
    "   when pc1.ig_level=1 then pc1.ig_name_en" +
    "   when pc2.ig_level=1 then pc2.ig_name_en" +
    "   when pc3.ig_level=1 then pc3.ig_name_en" +
    "   else ''" +
    " END as product_category_second_id_name_en" +
    " ,case" +
    "   when pc0.ig_level=2 then pc0.ig_id" +
    "   when pc1.ig_level=2 then pc1.ig_id" +
    "   when pc2.ig_level=2 then pc2.ig_id" +
    "   when pc3.ig_level=2 then pc3.ig_id" +
    "   else 0" +
    " END as product_category_third_id" +
    " ,case" +
    "   when pc0.ig_level=2 then pc0.ig_name" +
    "   when pc1.ig_level=2 then pc1.ig_name" +
    "   when pc2.ig_level=2 then pc2.ig_name" +
    "   when pc3.ig_level=2 then pc3.ig_name" +
    "   else ''" +
    " END as product_category_third_id_name" +
    " ,case" +
    "   when pc0.ig_level=2 then pc0.ig_name_en" +
    "   when pc1.ig_level=2 then pc1.ig_name_en" +
    "   when pc2.ig_level=2 then pc2.ig_name_en" +
    "   when pc3.ig_level=2 then pc3.ig_name_en" +
    "   else ''" +
    " END as product_category_third_id_name_en," +
    " null as product_category_lang," +
    " null as product_category_lang_val," +
    " null  as  product_sync_to_wms," +
    " null as  product_barcode_info," +
    " null as  product_UnitId," +
    " null as  product_ProductUrl," +
    " null  as  product_IsBrand," +
    " null as  product_IsBrand_val," +
    " null  as  product_IsDeployControl," +
    " null as  product_IsDeployControl_val," +
    " null  as  product_ReVerifyStatus," +
    " null as product_ReVerifyStatus_val," +
    " null as  product_Remark," +
    " prt.product_length as product_dm_length," +
    " prt.product_width as product_dm_width," +
    " prt.product_height as product_dm_height," +
    " prt.product_weight as product_dm_weight" +
    " from (SELECT * from ods.zy_wms_product as gos where gos.day="+ nowDate +") as prt" +
    " left join (SELECT * from ods.zy_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc0 on prt.pce_id = pc0.ig_id" +
    " left join (SELECT * from ods.zy_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc1 on pc1.ig_id  = pc0.ig_pid" +
    " left join (SELECT * from ods.zy_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc2 on pc2.ig_id  = pc1.ig_pid" +
    " left join (SELECT * from ods.zy_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc3 on pc3.ig_id  = pc2.ig_pid" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'product_status' " +
    "   AND vl_datasource_table = 'zy_wms_product'" +
    "   ) AS v_product_status ON prt.product_status = v_product_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'sale_status' " +
    "   AND vl_datasource_table = 'zy_wms_product'" +
    "   ) AS v_sale_status ON prt.sale_status = v_sale_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'product_receive_status'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_product_receive_status ON prt.product_receive_status = v_product_receive_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'product_is_qc'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_product_is_qc ON prt.product_is_qc = v_product_is_qc.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'product_barcode_type'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_product_barcode_type ON prt.product_barcode_type = v_product_barcode_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'product_type'" +
    "   AND vl_datasource_table = 'zy_wms_product'" +
    "   ) AS v_product_type ON prt.product_type = v_product_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'contain_battery'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_contain_battery ON prt.contain_battery = v_contain_battery.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'product_package_type'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_product_package_type ON prt.product_package_type = v_product_package_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'shared_sign'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_shared_sign ON prt.shared_sign = v_shared_sign.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9022' " +
    "   AND vl_type = 'is_thirdpart'" +
    "   AND vl_datasource_table = 'zy_wms_product' " +
    "   ) AS v_is_thirdpart ON prt.is_thirdpart = v_is_thirdpart.vl_value"
  private val gc_oms="select" +
    " prt.row_wid as row_wid," +
    " from_unixtime(unix_timestamp(current_date()), 'yyyyMMdd') as etl_proc_wid, " +
    " current_timestamp() as w_insert_dt, " +
    " current_timestamp() as w_update_dt, " +
    " prt.datasource_num_id as datasource_num_id," +
    " prt.data_flag as data_flag," +
    " prt.integration_id as integration_id," +
    " prt.created_on_dt as created_on_dt," +
    " prt.changed_on_dt as changed_on_dt," +
    " cast(CONCAT(prt.datasource_num_id, cp.c_id) as bigint) as customer_key," +
    " prt.product_id as product_id," +
    " prt.product_sku as product_sku," +
    " prt.product_barcode as product_barcode," +
    " prt.reference_no as product_reference_no," +
    " prt.customer_code as product_customer_code," +
    " cp.c_id as product_customer_id," +
    " regexp_replace(prt.product_title_en, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as product_title_en," +
    " regexp_replace(prt.product_title, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as product_title," +
    " prt.product_declared_name as product_declared_name," +
    " prt.product_status as product_status," +
    " nvl(v_product_status.vl_bi_name, prt.product_status) as product_status_val," +
    " prt.sale_status as product_sale_status," +
    " nvl(v_sale_status.vl_bi_name, prt.sale_status) as product_sale_status_val," +
    " prt.product_receive_status as product_receive_status," +
    " nvl(v_product_receive_status.vl_bi_name, prt.product_receive_status) as product_receive_status_val," +
    " prt.hs_code as product_hs_code," +
    " prt.currency_code as product_currency_code," +
    " prt.pu_code as product_pu_code ," +
    " prt.product_length as product_length," +
    " prt.product_width as product_width," +
    " prt.product_height as product_height," +
    " prt.product_net_weight as product_net_weight," +
    " prt.product_weight as product_weight," +
    " prt.product_sales_value as product_sales_value," +
    " prt.product_purchase_value as product_purchase_value," +
    " prt.product_declared_value as product_declared_value," +
    " prt.product_is_qc as product_is_qc," +
    " nvl(v_product_is_qc.vl_bi_name, prt.product_is_qc)as product_is_qc_val," +
    " prt.product_barcode_type as product_barcode_type," +
    " nvl(v_product_barcode_type.vl_bi_name, prt.product_barcode_type) as product_barcode_type_val," +
    " prt.product_type as product_type," +
    " nvl(v_product_type.vl_bi_name, prt.product_type) as product_type_val, " +
    " prt.pc_id as product_pc_id," +
    " prt.pce_id as product_pce_id," +
    " prt.product_add_time as product_add_time," +
    " prt.product_update_time as product_update_time," +
    " prt.have_asn as product_have_asn," +
    " prt.pd_id as product_pd_id," +
    " prt.contain_battery as product_product_attribute," +
    " nvl(v_contain_battery.vl_bi_name, prt.contain_battery) as product_product_attribute_val," +
    " prt.buyer_id as product_buyer_id ," +
    " prt.warning_qty as product_warning_qty ," +
    " prt.product_package_type as product_package_type," +
    " nvl(v_product_package_type.vl_bi_name, prt.product_package_type) as product_package_type_val," +
    " prt.shared_sign as product_is_shared_sign," +
    " nvl(v_shared_sign.vl_bi_name, prt.shared_sign) as product_is_shared_sign_val," +
    " prt.shared_unit_price as product_shared_unit_price," +
    " prt.product_describe as product_describe ," +
    " prt.shared_time as  product_shared_time," +
    " prt.reason as product_reason ," +
    " null as  product_customs_code," +
    " prt.product_declared_name_zh as product_declared_name_zh ," +
    " prt.create_type as product_create_type ," +
    " prt.is_thirdpart as product_is_thirdpart," +
    " nvl(v_is_thirdpart.vl_bi_name, prt.is_thirdpart) as product_is_thirdpart_val," +
    " prt.thirdpart_platform as product_thirdpart_platform ," +
    " prt.relation_barcode as product_relation_barcode ," +
    " null  as  product_mark_weight," +
    " null  as  product_mark_length," +
    " null  as  product_mark_width," +
    " null  as  product_mark_height," +
    " null  as  product_real_weight," +
    " null  as  product_real_height," +
    " null  as  product_real_width," +
    " null  as  product_real_length," +
    " null as product_abnormal_time," +
    " null as product_oms_add_time," +
    " prt.product_material as product_material ," +
    " prt.product_brand as product_brand ," +
    " prt.product_model as product_model ," +
    " prt.product_function as product_function ," +
    " prt.versions as product_versions," +
    " null as product_is_irregular," +
    " null as product_is_irregular_val," +
    " null as product_irregular_values ," +
    " prt.company_code as product_company_code," +
    " prt.product_desc as product_desc," +
    " prt.pce_id as product_category_id," +
    " case " +
    "   when pc0.ig_level=0 then pc0.ig_id" +
    "   when pc1.ig_level=0 then pc1.ig_id" +
    "   when pc2.ig_level=0 then pc2.ig_id" +
    "   when pc3.ig_level=0 then pc3.ig_id" +
    "   else 0" +
    " END as product_category_first_id," +
    " case  " +
    "   when pc0.ig_level=0 then pc0.ig_name" +
    "   when pc1.ig_level=0 then pc1.ig_name" +
    "   when pc2.ig_level=0 then pc2.ig_name" +
    "   when pc3.ig_level=0 then pc3.ig_name" +
    "   else ''" +
    " END as product_category_first_id_name," +
    " case   " +
    "   when pc0.ig_level=0 then pc0.ig_name_en" +
    "   when pc1.ig_level=0 then pc1.ig_name_en" +
    "   when pc2.ig_level=0 then pc2.ig_name_en" +
    "   when pc3.ig_level=0 then pc3.ig_name_en" +
    "   else ''" +
    " END as product_category_first_id_name_en," +
    " case " +
    "   when pc0.ig_level=1 then pc0.ig_id" +
    "   when pc1.ig_level=1 then pc1.ig_id" +
    "   when pc2.ig_level=1 then pc2.ig_id" +
    "   when pc3.ig_level=1 then pc3.ig_id" +
    "   else 0" +
    " END as product_category_second_id," +
    " case  " +
    "   when pc0.ig_level=1 then pc0.ig_name" +
    "   when pc1.ig_level=1 then pc1.ig_name" +
    "   when pc2.ig_level=1 then pc2.ig_name" +
    "   when pc3.ig_level=1 then pc3.ig_name" +
    "   else ''" +
    " END as product_category_second_id_name," +
    " case   " +
    "   when pc0.ig_level=1 then pc0.ig_name_en" +
    "   when pc1.ig_level=1 then pc1.ig_name_en" +
    "   when pc2.ig_level=1 then pc2.ig_name_en" +
    "   when pc3.ig_level=1 then pc3.ig_name_en" +
    "   else ''" +
    " END as product_category_second_id_name_en," +
    " case " +
    "   when pc0.ig_level=2 then pc0.ig_id" +
    "   when pc1.ig_level=2 then pc1.ig_id" +
    "   when pc2.ig_level=2 then pc2.ig_id" +
    "   when pc3.ig_level=2 then pc3.ig_id" +
    "   else 0" +
    " END as product_category_third_id," +
    " case  " +
    "   when pc0.ig_level=2 then pc0.ig_name" +
    "   when pc1.ig_level=2 then pc1.ig_name" +
    "   when pc2.ig_level=2 then pc2.ig_name" +
    "   when pc3.ig_level=2 then pc3.ig_name" +
    "   else ''" +
    " END as product_category_third_id_name," +
    " case   " +
    "   when pc0.ig_level=2 then pc0.ig_name_en" +
    "   when pc1.ig_level=2 then pc1.ig_name_en" +
    "   when pc2.ig_level=2 then pc2.ig_name_en" +
    "   when pc3.ig_level=2 then pc3.ig_name_en" +
    "   else ''" +
    " END as product_category_third_id_name_en," +
    " prt.cat_lang as product_category_lang," +
    " nvl(v_cat_lang.vl_bi_name, prt.cat_lang) as product_category_lang_val," +
    " prt.sync_to_wms as product_sync_to_wms ," +
    " prt.barcode_info as product_barcode_info," +
    " null as product_UnitId ," +
    " null as product_ProductUrl ," +
    " null as product_IsBrand," +
    " null as product_IsBrand_val," +
    " null as product_IsDeployControl," +
    " null as product_IsDeployControl_val," +
    " null  as product_ReVerifyStatus," +
    " null as product_ReVerifyStatus_val," +
    " null as product_Remark," +
    " prt.product_length as product_dm_length," +
    " prt.product_width as product_dm_width," +
    " prt.product_height as product_dm_height," +
    " prt.product_weight as product_dm_weight" +
    " from (SELECT * from ods.gc_oms_product as gos where gos.day="+ nowDate +") as prt" +
    " left join (SELECT * from ods.gc_oms_company as gos where gos.day="+ nowDate +") as cp on prt.company_code = cp.company_code" +
    " left join (SELECT * from ods.gc_oms_product_category_oms as gos where gos.day="+ nowDate +") as pc0 on prt.pce_id = pc0.ig_id" +
    " left join (SELECT * from ods.gc_oms_product_category_oms as gos where gos.day="+ nowDate +") as pc1 on pc1.ig_id  = pc0.ig_pid" +
    " left join (SELECT * from ods.gc_oms_product_category_oms as gos where gos.day="+ nowDate +") as pc2 on pc2.ig_id  = pc1.ig_pid" +
    " left join (SELECT * from ods.gc_oms_product_category_oms as gos where gos.day="+ nowDate +") as pc3 on pc3.ig_id  = pc2.ig_pid" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'product_status'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_product_status ON prt.product_status = v_product_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'sale_status'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_sale_status ON prt.sale_status = v_sale_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'product_receive_status'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_product_receive_status ON prt.product_receive_status = v_product_receive_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'product_is_qc'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_product_is_qc ON prt.product_is_qc = v_product_is_qc.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'product_barcode_type'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_product_barcode_type ON prt.product_barcode_type = v_product_barcode_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'product_type'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_product_type ON prt.product_type = v_product_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'contain_battery'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_contain_battery ON prt.contain_battery = v_contain_battery.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'product_package_type'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_product_package_type ON prt.product_package_type = v_product_package_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'shared_sign'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_shared_sign ON prt.shared_sign = v_shared_sign.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'is_thirdpart'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_is_thirdpart ON prt.is_thirdpart = v_is_thirdpart.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9001' " +
    "   AND vl_type = 'cat_lang'" +
    "   AND vl_datasource_table = 'gc_oms_product' " +
    "   ) AS v_cat_lang ON prt.cat_lang = v_cat_lang.vl_value"
  private val gc_wms="select" +
    "   prt.row_wid as row_wid," +
    "   from_unixtime(unix_timestamp(current_date()), 'yyyyMMdd') as etl_proc_wid, " +
    "   current_timestamp() as w_insert_dt, " +
    "   current_timestamp() as w_update_dt, " +
    "   prt.datasource_num_id as datasource_num_id," +
    "   prt.data_flag as data_flag," +
    "   prt.integration_id as integration_id," +
    "   prt.created_on_dt as created_on_dt," +
    "   prt.changed_on_dt as changed_on_dt," +
    "  cast(CONCAT(prt.datasource_num_id, prt.customer_id) as bigint) as customer_key," +
    " prt.product_id as product_id ," +
    " prt.product_sku as product_sku ," +
    " prt.product_barcode as product_barcode ," +
    " prt.reference_no as product_reference_no ," +
    " prt.customer_code as product_customer_code ," +
    " prt.customer_id as product_customer_id ," +
    " regexp_replace(prt.product_title_en, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as product_title_en ," +
    " regexp_replace(prt.product_title, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as  product_title ," +
    " prt.product_declared_name   as  product_declared_name," +
    " prt.product_status as product_status," +
    " nvl(v_product_status.vl_bi_name, prt.product_status) as product_status_val," +
    " prt.sale_status as product_sale_status," +
    " nvl(null,prt.sale_status) as product_sale_status_val," +
    " prt.product_receive_status as product_receive_status," +
    " nvl(v_product_receive_status.vl_bi_name, prt.product_receive_status) as product_receive_status_val," +
    " prt.hs_code   as  product_hs_code," +
    " prt.currency_code   as  product_currency_code ," +
    " prt.pu_code   as  product_pu_code ," +
    " prt.product_length  as  product_length," +
    " prt.product_width as  product_width," +
    " prt.product_height  as  product_height," +
    " prt.product_net_weight  as  product_net_weight," +
    " prt.product_weight  as  product_weight," +
    " prt.product_sales_value as  product_sales_value," +
    " prt.product_purchase_value  as  product_purchase_value," +
    " prt.product_declared_value  as  product_declared_value," +
    " prt.product_is_qc as product_is_qc," +
    " prt.product_is_qc as product_is_qc_val," +
    " prt.product_barcode_type as product_barcode_type," +
    " prt.product_barcode_type as product_barcode_type_val," +
    " prt.product_type as product_type," +
    " prt.product_type as product_type_val," +
    " prt.pc_id as product_pc_id ," +
    " prt.pce_id as product_pce_id ," +
    " prt.product_add_time as product_add_time," +
    " prt.product_update_time as product_update_time," +
    " prt.have_asn as product_have_asn ," +
    " prt.pd_id as product_pd_id ," +
    " prt.contain_battery as product_product_attribute," +
    " nvl(v_contain_battery.vl_bi_name, prt.contain_battery) as product_product_attribute_val," +
    " prt.buyer_id  as  product_buyer_id ," +
    " prt.warning_qty   as  product_warning_qty ," +
    " prt.product_package_type as product_package_type," +
    " nvl(v_product_package_type.vl_bi_name, prt.product_package_type) as product_package_type_val,   " +
    " prt.shared_sign as product_is_shared_sign," +
    " prt.shared_sign as product_is_shared_sign_val," +
    " prt.shared_unit_price as  product_shared_unit_price," +
    " prt.product_describe  as  product_describe ," +
    " prt.shared_time as  product_shared_time," +
    " prt.reason as product_reason ," +
    " prt.customs_code  as  product_customs_code ," +
    " prt.product_declared_name_zh  as  product_declared_name_zh ," +
    " prt.create_type as product_create_type ," +
    " prt.is_thirdpart as product_is_thirdpart," +
    " nvl(v_is_thirdpart.vl_bi_name, prt.is_thirdpart) as product_is_thirdpart_val," +
    " prt.thirdpart_platform  as  product_thirdpart_platform," +
    " prt.relation_barcode  as  product_relation_barcode ," +
    " prt.product_mark_weight as  product_mark_weight," +
    " prt.product_mark_length as  product_mark_length," +
    " prt.product_mark_width  as  product_mark_width," +
    " prt.product_mark_height as  product_mark_height," +
    " prt.product_real_weight as  product_real_weight," +
    " prt.product_real_height as  product_real_height," +
    " prt.product_real_width  as  product_real_width," +
    " prt.product_real_length as  product_real_length," +
    " prt.product_abnormal_time as  product_abnormal_time," +
    " prt.product_oms_add_time  as  product_oms_add_time," +
    " prt.product_material  as  product_material ," +
    " prt.product_brand  as  product_brand ," +
    " prt.product_model   as  product_model ," +
    " prt.product_function as  product_function ," +
    " prt.versions  as  product_versions," +
    " prt.is_irregular as product_is_irregular," +
    " nvl(v_is_irregular.vl_bi_name, prt.is_irregular) as product_is_irregular_val," +
    " prt.irregular_values as product_irregular_values," +
    " null as  product_company_code," +
    " null as  product_desc," +
    " prt.pce_id as product_category_id," +
    " case " +
    "     when pc0.ig_level=0 then pc0.ig_id" +
    "     when pc1.ig_level=0 then pc1.ig_id" +
    "     when pc2.ig_level=0 then pc2.ig_id" +
    "     when pc3.ig_level=0 then pc3.ig_id" +
    "     else 0" +
    " END as product_category_first_id," +
    " case " +
    "     when pc0.ig_level=0 then pc0.ig_name" +
    "     when pc1.ig_level=0 then pc1.ig_name" +
    "     when pc2.ig_level=0 then pc2.ig_name" +
    "     when pc3.ig_level=0 then pc3.ig_name" +
    "     else ''" +
    " END as product_category_first_id_name," +
    " case " +
    "     when pc0.ig_level=0 then pc0.ig_name_en" +
    "     when pc1.ig_level=0 then pc1.ig_name_en" +
    "     when pc2.ig_level=0 then pc2.ig_name_en" +
    "     when pc3.ig_level=0 then pc3.ig_name_en" +
    "     else ''" +
    " END as product_category_first_id_name_en," +
    " case  " +
    "     when pc0.ig_level=1 then pc0.ig_id" +
    "     when pc1.ig_level=1 then pc1.ig_id" +
    "     when pc2.ig_level=1 then pc2.ig_id" +
    "     when pc3.ig_level=1 then pc3.ig_id" +
    "     else 0" +
    " END as product_category_second_id," +
    " case  " +
    "     when pc0.ig_level=1 then pc0.ig_name" +
    "     when pc1.ig_level=1 then pc1.ig_name" +
    "     when pc2.ig_level=1 then pc2.ig_name" +
    "     when pc3.ig_level=1 then pc3.ig_name" +
    "     else 0" +
    " END as product_category_second_id_name," +
    " case  " +
    "     when pc0.ig_level=1 then pc0.ig_name_en" +
    "     when pc1.ig_level=1 then pc1.ig_name_en" +
    "     when pc2.ig_level=1 then pc2.ig_name_en" +
    "     when pc3.ig_level=1 then pc3.ig_name_en" +
    "     else 0" +
    " END as product_category_second_id_name_en," +
    " case   " +
    "     when pc0.ig_level=2 then pc0.ig_id" +
    "     when pc1.ig_level=2 then pc1.ig_id" +
    "     when pc2.ig_level=2 then pc2.ig_id" +
    "     when pc3.ig_level=2 then pc3.ig_id" +
    "     else 0" +
    " END as product_category_third_id," +
    " case   " +
    "     when pc0.ig_level=2 then pc0.ig_name" +
    "     when pc1.ig_level=2 then pc1.ig_name" +
    "     when pc2.ig_level=2 then pc2.ig_name" +
    "     when pc3.ig_level=2 then pc3.ig_name" +
    "     else 0" +
    " END as product_category_third_id_name," +
    " case   " +
    "     when pc0.ig_level=2 then pc0.ig_name_en" +
    "     when pc1.ig_level=2 then pc1.ig_name_en" +
    "     when pc2.ig_level=2 then pc2.ig_name_en" +
    "     when pc3.ig_level=2 then pc3.ig_name_en" +
    "     else 0" +
    " END as product_category_third_id_name_en," +
    " null as product_category_lang," +
    " null as product_category_lang_val," +
    " null  as product_sync_to_wms," +
    " null as product_barcode_info ," +
    " null as product_UnitId ," +
    " null as product_ProductUrl ," +
    " null as product_IsBrand," +
    " null as product_IsBrand_val," +
    " null as product_IsDeployControl," +
    " null as product_IsDeployControl_val," +
    " null  as product_ReVerifyStatus," +
    " null as product_ReVerifyStatus_val," +
    " null as  product_Remark," +
    " CASE " +
    "  WHEN (" +
    "  prt.product_real_length + prt.product_real_width + prt.product_real_height > 0 " +
    "  AND prt.product_real_length * prt.product_real_width * prt.product_real_height > 0 " +
    "  ) THEN" +
    "  nvl ( prt.product_real_length, prt.product_length ) ELSE prt.product_length " +
    "  END as product_dm_length," +
    " CASE " +
    "  WHEN (" +
    "  prt.product_real_length + prt.product_real_width + prt.product_real_height > 0 " +
    "  AND prt.product_real_length * prt.product_real_width * prt.product_real_height > 0 " +
    "  ) THEN" +
    "  nvl ( prt.product_real_width , prt.product_width) ELSE prt.product_width " +
    "  END as product_dm_width," +
    " CASE " +
    "  WHEN (" +
    "  prt.product_real_length + prt.product_real_width + prt.product_real_height > 0 " +
    "  AND prt.product_real_length * prt.product_real_width * prt.product_real_height > 0 " +
    "  ) THEN" +
    "  nvl ( prt.product_real_height  , prt.product_height) ELSE prt.product_height" +
    "  END as product_dm_height," +
    " CASE " +
    "  WHEN (" +
    "  prt.product_real_length + prt.product_real_width + prt.product_real_height > 0 " +
    "  AND prt.product_real_length * prt.product_real_width * prt.product_real_height > 0 " +
    "  ) THEN" +
    "  nvl ( prt.product_real_weight, prt.product_weight) ELSE prt.product_weight" +
    "  END as product_dm_weight" +
    " from ( select wmsP.* " +
    "   FROM (SELECT * from ods.gc_wms_product as gos where gos.day="+ nowDate +") wmsP left join (SELECT * from ods.gc_bsc_product as gos where gos.day= "+ nowDate + ") bscP on wmsP.product_id = bscP.ProductId " +
    "   WHERE bscP.ProductId is null" +
    " )  prt" +
    "  LEFT JOIN (SELECT * from ods.gc_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc0 on pc0.ig_id=prt.pce_id" +
    " left join (SELECT * from ods.gc_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc1 on pc1.ig_id=pc0.ig_pid" +
    " left join (SELECT * from ods.gc_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc2 on pc2.ig_id=pc1.ig_pid" +
    " left join (SELECT * from ods.gc_wms_product_category_oms as gos where gos.day="+ nowDate +") as pc3 on pc3.ig_id=pc2.ig_pid" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9004' " +
    "   AND vl_type = 'product_status'" +
    "   AND vl_datasource_table = 'gc_wms_product' " +
    "   ) AS v_product_status ON prt.product_status = v_product_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9004' " +
    "   AND vl_type = 'product_receive_status'" +
    "   AND vl_datasource_table = 'gc_wms_product' " +
    "   ) AS v_product_receive_status ON prt.product_receive_status = v_product_receive_status.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9004' " +
    "   AND vl_type = 'contain_battery'" +
    "   AND vl_datasource_table = 'gc_wms_product' " +
    "   ) AS v_contain_battery ON prt.contain_battery = v_contain_battery.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9004' " +
    "   AND vl_type = 'product_package_type'" +
    "   AND vl_datasource_table = 'gc_wms_product' " +
    "   ) AS v_product_package_type ON prt.contain_battery = v_product_package_type.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9004' " +
    "   AND vl_type = 'is_thirdpart'" +
    "   AND vl_datasource_table = 'gc_wms_product' " +
    "   ) AS v_is_thirdpart ON prt.is_thirdpart = v_is_thirdpart.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9004' " +
    "   AND vl_type = 'is_irregular'" +
    "   AND vl_datasource_table = 'gc_wms_product' " +
    "   ) AS v_is_irregular ON prt.is_irregular = v_is_irregular.vl_value"
  private val gc_bsc="select" +
    " cast(concat('9004',prt.productid) as bigint) as row_wid," +
    " from_unixtime(unix_timestamp(current_date()), 'yyyyMMdd') as etl_proc_wid, " +
    " current_timestamp() as w_insert_dt, " +
    " current_timestamp() as w_update_dt, " +
    " '9004' as datasource_num_id," +
    " prt.data_flag as data_flag," +
    " prt.integration_id as integration_id," +
    " prt.created_on_dt as created_on_dt," +
    " prt.changed_on_dt as changed_on_dt," +
    " cast(CONCAT('9004', cus.CustomerId) as bigint) as customer_key," +
    " prt.ProductId as  product_id ," +
    " prt.ProductSku as product_sku ," +
    " prt.ProductBarcode  as  product_barcode ," +
    " prt.ReferenceNumber   as  product_reference_no ," +
    " prt.CustomerCode  as  product_customer_code ," +
    " cus.CustomerId  as  product_customer_id ," +
    " regexp_replace(gc_wms_product.product_title_en, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as  product_title_en ," +
    " regexp_replace(prt.ProductName, '\\\\\\\\r|\\\\\\\\n|\\\\\\\\t','') as product_title ," +
    " prt.ProductDeclaredName as product_declared_name ," +
    " prt.ProductStatus as product_status," +
    " nvl(v_ProductStatus.vl_bi_name, prt.ProductStatus) as product_status_val," +
    " gc_wms_product.sale_status as product_sale_status," +
    " nvl(null,gc_wms_product.sale_status) as product_sale_status_val," +
    " prt.ReceiveStatus as product_receive_status," +
    " nvl(v_ReceiveStatus.vl_bi_name, prt.ReceiveStatus) as product_receive_status_val," +
    " prt.HSCode  as  product_hs_code," +
    " gc_wms_product.currency_code  as  product_currency_code ," +
    " gc_wms_product.pu_code  as  product_pu_code ," +
    " prt.ProductLength as  product_length," +
    " prt.ProductWidth  as  product_width," +
    " prt.ProductHeight as  product_height," +
    " gc_wms_product.product_net_weight as  product_net_weight," +
    " prt.ProductWeight as  product_weight," +
    " gc_wms_product.product_sales_value as product_sales_value," +
    " gc_wms_product.product_purchase_value as product_purchase_value," +
    " prt.ProductDeclaredValue as  product_declared_value," +
    " gc_wms_product.product_is_qc as product_is_qc," +
    " gc_wms_product.product_is_qc as product_is_qc_val," +
    " gc_wms_product.product_barcode_type as product_barcode_type," +
    " nvl(null,gc_wms_product.product_barcode_type) as product_barcode_type_val," +
    " gc_wms_product.product_type as product_type," +
    " gc_wms_product.product_type as product_type_val," +
    " gc_wms_product.pc_id as product_pc_id," +
    " gc_wms_product.pce_id as product_pce_id ," +
    " prt.AddTime as  product_add_time," +
    " prt.UpdateTime  as  product_update_time," +
    " gc_wms_product.have_asn   as  product_have_asn ," +
    " gc_wms_product.pd_id  as  product_pd_id ," +
    " prt.GoodsAttribute as product_product_attribute," +
    " nvl(v_GoodsAttribute.vl_bi_name, prt.GoodsAttribute) as product_product_attribute_val," +
    " gc_wms_product.buyer_id as  product_buyer_id ," +
    " prt.InventoryWarningQty as  product_warning_qty ," +
    " prt.ProductPackageType as product_package_type," +
    " nvl(v_ProductPackageType.vl_bi_name, prt.ProductPackageType) as product_package_type_val," +
    " gc_wms_product.shared_sign as product_is_shared_sign," +
    " gc_wms_product.shared_sign as product_is_shared_sign_val," +
    " gc_wms_product.shared_unit_price  as  product_shared_unit_price," +
    " gc_wms_product.product_describe  as  product_describe ," +
    " gc_wms_product.shared_time  as  product_shared_time," +
    " gc_wms_product.reason   as  product_reason ," +
    " gc_wms_product.customs_code   as  product_customs_code ," +
    " prt.ProductDeclaredNameCN   as  product_declared_name_zh ," +
    " gc_wms_product.create_type as  product_create_type ," +
    " prt.IsThirdPart as product_is_thirdpart," +
    " nvl(v_IsThirdPart.vl_bi_name, prt.IsThirdPart) as product_is_thirdpart_val," +
    " prt.ThirdPartPlatform   as  product_thirdpart_platform ," +
    " prt.RelationBarcode   as  product_relation_barcode ," +
    " gc_wms_product.product_mark_weight   as  product_mark_weight," +
    " gc_wms_product.product_mark_length  as  product_mark_length," +
    " gc_wms_product.product_mark_width as  product_mark_width," +
    " gc_wms_product.product_mark_height  as  product_mark_height," +
    " prt.ReceiveWeight as  product_real_weight," +
    " prt.ReceiveHeight   as  product_real_height," +
    " prt.ReceiveWidth  as  product_real_width," +
    " prt.ReceiveLength as  product_real_length," +
    " null as product_abnormal_time," +
    " null as product_oms_add_time," +
    " prt.ProductMaterial   as  product_material ," +
    " prt.ProductBrand  as  product_brand ," +
    " prt.ProductModel  as  product_model ," +
    " prt.ProductFunction   as  product_function ," +
    " gc_wms_product.versions as product_versions," +
    " ext.IsIrregular as product_is_irregular," +
    " nvl(v_IsIrregular.vl_bi_name, ext.IsIrregular) as product_is_irregular_val," +
    " ext.IrregularValue as product_irregular_values," +
    " null as  product_company_code," +
    " null as  product_desc," +
    " prt.CategoryFirstId  as  product_category_id," +
    " case " +
    "     when pc1.CategoryLevel=0 then pc1.ProductCategoryId" +
    "     when pc2.CategoryLevel=0 then pc2.ProductCategoryId" +
    "     when pc3.CategoryLevel=0 then pc3.ProductCategoryId" +
    "     else 0" +
    " END as product_category_first_id," +
    " case " +
    "     when pc1.CategoryLevel=0 then pc1.CategoryName" +
    "     when pc2.CategoryLevel=0 then pc2.CategoryName" +
    "     when pc3.CategoryLevel=0 then pc3.CategoryName" +
    "     else ''" +
    " END as product_category_first_id_name," +
    " case " +
    "     when pc1.CategoryLevel=0 then pc1.CategoryNameEn" +
    "     when pc2.CategoryLevel=0 then pc2.CategoryNameEn" +
    "     when pc3.CategoryLevel=0 then pc3.CategoryNameEn" +
    "     else ''" +
    " END as product_category_first_id_name_en," +
    " case " +
    "     when pc1.CategoryLevel=1 then pc1.ProductCategoryId" +
    "     when pc2.CategoryLevel=1 then pc2.ProductCategoryId" +
    "     when pc3.CategoryLevel=1 then pc3.ProductCategoryId" +
    "     else 0" +
    " END as product_category_second_id," +
    " case " +
    "     when pc1.CategoryLevel=1 then pc1.CategoryName" +
    "     when pc2.CategoryLevel=1 then pc2.CategoryName" +
    "     when pc3.CategoryLevel=1 then pc3.CategoryName" +
    "     else ''" +
    " END as product_category_second_id_name," +
    " case " +
    "     when pc1.CategoryLevel=1 then pc1.CategoryNameEn" +
    "     when pc2.CategoryLevel=1 then pc2.CategoryNameEn" +
    "     when pc3.CategoryLevel=1 then pc3.CategoryNameEn" +
    "     else ''" +
    " END as product_category_second_id_name_en," +
    " case " +
    "     when pc1.CategoryLevel=2 then pc1.ProductCategoryId" +
    "     when pc2.CategoryLevel=2 then pc2.ProductCategoryId" +
    "     when pc3.CategoryLevel=2 then pc3.ProductCategoryId" +
    "     else 0" +
    " END as product_category_third_id," +
    " case " +
    "     when pc1.CategoryLevel=2 then pc1.CategoryName" +
    "     when pc2.CategoryLevel=2 then pc2.CategoryName" +
    "     when pc3.CategoryLevel=2 then pc3.CategoryName" +
    "     else ''" +
    " END as product_category_third_id_name," +
    " case " +
    "     when pc1.CategoryLevel=2 then pc1.CategoryNameEn" +
    "     when pc2.CategoryLevel=2 then pc2.CategoryNameEn" +
    "     when pc3.CategoryLevel=2 then pc3.CategoryNameEn" +
    "     else ''" +
    " END as product_category_third_id_name_en," +
    " prt.CategoryLanguage as product_category_lang," +
    " nvl(v_CategoryLanguage.vl_bi_name, prt.CategoryLanguage) as product_category_lang_val," +
    " null as product_sync_to_wms," +
    " null as product_barcode_info," +
    " prt.UnitId  as product_UnitId ," +
    " prt.ProductUrl as product_ProductUrl ," +
    " prt.IsBrand as product_IsBrand," +
    " nvl(v_IsBrand.vl_bi_name, prt.IsBrand) as product_IsBrand_val," +
    " prt.IsDeployControl as product_IsDeployControl," +
    " nvl(v_IsDeployControl.vl_bi_name, prt.IsDeployControl) as product_IsDeployControl_val," +
    " prt.ReVerifyStatus as product_ReVerifyStatus," +
    " nvl(v_ReVerifyStatus.vl_bi_name, prt.ReVerifyStatus) as product_ReVerifyStatus_val," +
    " prt.Remark as product_Remark," +
    " CASE " +
    "  WHEN (" +
    "  prt.ReceiveLength + prt.ReceiveWidth + prt.ReceiveHeight > 0 " +
    "  AND prt.ReceiveLength * prt.ReceiveWidth * prt.ReceiveHeight > 0 " +
    "  ) THEN" +
    "  nvl ( prt.ReceiveLength, prt.ProductLength ) ELSE prt.ProductLength " +
    "  END as product_dm_length," +
    " CASE " +
    "  WHEN (" +
    "  prt.ReceiveLength + prt.ReceiveWidth + prt.ReceiveHeight > 0 " +
    "  AND prt.ReceiveLength * prt.ReceiveWidth * prt.ReceiveHeight > 0 " +
    "  ) THEN" +
    "  nvl ( prt.ReceiveWidth , prt.ProductWidth) ELSE prt.ProductWidth " +
    "  END as product_dm_width," +
    " CASE " +
    "  WHEN (" +
    "  prt.ReceiveLength + prt.ReceiveWidth + prt.ReceiveHeight > 0 " +
    "  AND prt.ReceiveLength * prt.ReceiveWidth * prt.ReceiveHeight > 0 " +
    "  ) THEN" +
    "  nvl ( prt.ReceiveHeight  , prt.ProductHeight) ELSE prt.ProductHeight" +
    "  END as product_dm_height," +
    " CASE " +
    "  WHEN (" +
    "  prt.ReceiveLength + prt.ReceiveWidth + prt.ReceiveHeight > 0 " +
    "  AND prt.ReceiveLength * prt.ReceiveWidth * prt.ReceiveHeight > 0 " +
    "  ) THEN" +
    "  nvl ( prt.ReceiveWeight, prt.ProductWeight) ELSE prt.ProductWeight" +
    "  END as product_dm_weight" +
    " FROM (SELECT * from ods.gc_bsc_product as gos where gos.day="+ nowDate +") as prt" +
    " left join (SELECT * from ods.gc_wms_product as gos where gos.day="+ nowDate +") as gc_wms_product on prt.ProductId = gc_wms_product.product_id" +
    " left join (SELECT * from ods.gc_bsc_customer as gos where gos.day="+ nowDate +") as cus on prt.CustomerCode = cus.CustomerCode" +
    " left join (SELECT * from ods.gc_bsc_productcategory as gos where gos.day="+ nowDate +") as pc1 on prt.CategoryFirstId  = pc1.ProductCategoryId" +
    " left join (SELECT * from ods.gc_bsc_productcategory as gos where gos.day="+ nowDate +") as  pc2 on prt.CategorySecondId = pc2.ProductCategoryId" +
    " left join (SELECT * from ods.gc_bsc_productcategory as gos where gos.day="+ nowDate +") as pc3 on prt.CategoryThirdId  = pc3.ProductCategoryId" +
    " LEFT JOIN (SELECT * from ods.gc_bsc_productExtend as gos where gos.day="+ nowDate +")   as ext on prt.ProductId = ext.ProductId" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'ProductStatus'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    "   ) AS v_ProductStatus ON prt.ProductStatus = v_ProductStatus.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'ReceiveStatus'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_ReceiveStatus ON prt.ReceiveStatus = v_ReceiveStatus.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'GoodsAttribute'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_GoodsAttribute ON prt.GoodsAttribute = v_GoodsAttribute.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'ProductPackageType'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_ProductPackageType ON prt.ProductPackageType = v_ProductPackageType.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'IsThirdPart'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_IsThirdPart ON prt.IsThirdPart = v_IsThirdPart.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'IsIrregular'" +
    "   AND vl_datasource_table = 'gc_bsc_productExtend' " +
    " ) AS v_IsIrregular ON ext.IsIrregular = v_IsIrregular.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'CategoryLanguage'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_CategoryLanguage ON prt.CategoryLanguage = v_CategoryLanguage.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'IsBrand'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_IsBrand ON prt.IsBrand = v_IsBrand.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'IsDeployControl'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_IsDeployControl ON prt.IsDeployControl = v_IsDeployControl.vl_value" +
    " LEFT JOIN ( " +
    "   SELECT * FROM dw.par_val_list " +
    "   WHERE datasource_num_id = '9021' " +
    "   AND vl_type = 'ReVerifyStatus'" +
    "   AND vl_datasource_table = 'gc_bsc_product' " +
    " ) AS v_ReVerifyStatus ON prt.ReVerifyStatus = v_ReVerifyStatus.vl_value"

  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(zy_wms,gc_oms,gc_wms,gc_bsc).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task, tableName, sqlArray, Array(SystemCodeUtil.ZY_WMS,SystemCodeUtil.GC_OMS,SystemCodeUtil.GC_WMS,SystemCodeUtil.GC_BSC))

  }

}
