package com.zongteng.ztetl.etl.dw.dim.all

import com.zongteng.ztetl.common.{Dw_dim_common, Dw_par_val_list_cache, SystemCodeUtil}
import com.zongteng.ztetl.util.DateUtil

object DimShippingMethod {

  //任务名称(一般同类名)
  private val task = "Dw_dim_shipping_method"

  //dw层类名
  private val tableName = "dim_shipping_method"

  //获取当天的时间
  private val nowDate: String = DateUtil.getNowTime()

  //要执行的sql语句
  private val gc_wms="select " +
    " sm.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " sm.datasource_num_id AS datasource_num_id," +
    " sm.data_flag AS data_flag," +
    " sm.integration_id AS integration_id," +
    " sm.created_on_dt AS created_on_dt," +
    " sm.changed_on_dt AS changed_on_dt," +
    " cast(CONCAT(sm.datasource_num_id, sm.warehouse_id) as bigint) AS warehouse_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.sc_id) as bigint) AS sc_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.st_id) as bigint) AS st_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.sct_id) as bigint) AS sct_key, " +
    " cast(CONCAT(sm.datasource_num_id, sm.sm_transfer_warehouse_id) as bigint) AS sm_transfer_warehouse_key," +
    " sm.sm_id AS sm_id," +
    " sm.sm_code AS sm_code," +
    " sm.sm_short_name AS sm_short_name," +
    " sm.sm_name_cn AS sm_name_cn," +
    " sm.sm_name AS sm_name," +
    " sm.pg_code AS sm_pg_code," +
    " sm.sm_mp_fee AS sm_mp_fee," +
    " sm.sm_reg_fee AS sm_reg_fee," +
    " sm.sm_addons AS sm_addons," +
    " sm.sm_type AS sm_type," +
    " nvl(v_sm_type.vl_bi_name, sm.sm_type) AS sm_type_val," +
    " sm.sm_baf AS sm_baf," +
    " sm.sm_discount AS sm_discount," +
    " sm.sm_delivery_time_min AS sm_delivery_time_min," +
    " sm.sm_delivery_time_max AS sm_delivery_time_max," +
    " sm.sm_delivery_time_avg AS sm_delivery_time_avg," +
    " sm.sm_is_volume AS sm_is_volume," +
    " nvl(v_sm_is_volume.vl_bi_name, sm.sm_is_volume) AS sm_is_volume_val," +
    " sm.sm_is_charge AS sm_is_charge," +
    " nvl(v_sm_is_charge.vl_bi_name, sm.sm_is_charge) AS sm_is_charge_val," +
    " sm.sm_vol_rate  AS sm_vol_rate," +
    " sm.sm_status AS sm_status," +
    " nvl(v_sm_status.vl_bi_name, sm.sm_status) AS sm_status_val," +
    " sm.sm_class_code AS sm_class_code," +
    " nvl(v_sm_class_code.vl_bi_name, sm.sm_class_code) AS sm_class_code_val," +
    " sm.sm_logo AS sm_logo," +
    " sm.sm_return_recipient AS sm_return_recipient," +
    " sm.sm_return_address AS sm_return_address," +
    " sm.sm_discount_min AS sm_discount_min," +
    " sm.sm_mp_fee_min AS sm_mp_fee_min," +
    " sm.sm_reg_fee_min AS sm_reg_fee_min," +
    " sm.sm_limit_volume AS sm_limit_volume," +
    " sm.sm_limit_weight AS sm_limit_weight," +
    " sm.sm_sort AS sm_sort," +
    " sm.sm_is_tracking AS sm_is_tracking," +
    " nvl(v_sm_is_tracking.vl_bi_name, sm.sm_is_tracking) AS sm_is_tracking_val," +
    " sm.sm_is_signature AS sm_is_signature," +
    " nvl(v_sm_is_signature.vl_bi_name, sm.sm_is_signature) AS sm_is_signature_val," +
    " sm.sm_is_insurance AS sm_is_insurance," +
    " nvl(v_sm_is_insurance.vl_bi_name, sm.sm_is_insurance) AS sm_is_insurance_val," +
    " sm.sm_is_validate_remote AS sm_is_validate_remote," +
    " nvl(v_sm_is_validate_remote.vl_bi_name, sm.sm_is_validate_remote) AS sm_is_validate_remote_val," +
    " sm.warehouse_id AS sm_warehouse_id," +
    " sm.sm_calc_type AS sm_calc_type," +
    " sm.sm_fee_type AS sm_fee_type," +
    " sm.sc_id AS sm_sc_id," +
    " sm.st_id AS sm_st_id," +
    " sm.sm_carrier_number AS sm_carrier_number," +
    " sm.sm_allow_letter AS sm_allow_letter," +
    " nvl(v_sm_allow_letter.vl_bi_name, sm.sm_allow_letter) AS sm_allow_letter_val," +
    " sm.sct_id AS sm_sct_id," +
    " sm.sm_update_time AS sm_update_time," +
    " sm.pre_generate_label AS sm_pre_generate_label," +
    " nvl(v_pre_generate_label.vl_bi_name, sm.pre_generate_label) AS sm_pre_generate_label_val," +
    " sm.sm_code_type AS sm_code_type," +
    " nvl(v_sm_code_type.vl_bi_name, sm.sm_code_type) AS sm_code_type_val," +
    " sm.sm_is_more_box AS sm_is_more_box," +
    " nvl(v_sm_is_more_box.vl_bi_name, sm.sm_is_more_box) AS sm_is_more_box_val," +
    " sm.sm_return_type AS sm_return_type," +
    " sm.sm_attribute AS sm_attribute," +
    " sm.sm_attribute AS sm_attribute_val," +
    " sm.sm_is_claim  AS sm_is_claim," +
    " nvl(v_sm_is_claim.vl_bi_name, sm.sm_is_claim) AS sm_is_claim_val," +
    " sm.sm_transfer_warehouse_id AS sm_transfer_warehouse_id," +
    " sm.sm_age_detection  AS sm_age_detection," +
    " nvl(v_sm_age_detection.vl_bi_name, sm.sm_age_detection) AS sm_age_detection_val," +
    " sm.sm_recommend_platform AS sm_recommend_platform," +
    " sm.sm_volume_unit AS sm_volume_unit," +
    " sm.sm_volume_type AS sm_volume_type," +
    " nvl(v_sm_volume_type.vl_bi_name, sm.sm_volume_type) AS sm_volume_type_val," +
    " sm.sm_volume_value AS sm_volume_value," +
    " sm.sm_weight_unit AS sm_weight_unit," +
    " sm.sm_weight_type  AS sm_weight_type," +
    " nvl(v_sm_weight_type.vl_bi_name, sm.sm_weight_type)  AS sm_weight_type_val," +
    " sm.sm_weight_value AS sm_weight_value," +
    " sm.claim_desc AS sm_claim_desc," +
    " sm.sfp_label_type AS sm_sfp_label_type," +
    " nvl(v_sfp_label_type.vl_bi_name, sm.sfp_label_type) AS sm_sfp_label_type_val," +
    " sm.life_gate AS sm_life_gate," +
    " nvl(v_life_gate.vl_bi_name, sm.life_gate) AS sm_life_gate_val," +
    " sm.sm_truck AS sm_truck," +
    " nvl(v_sm_truck.vl_bi_name, sm.sm_truck) AS sm_truck_val" +
    " from (SELECT * from ODS.gc_wms_shipping_method as gos where gos.day="+ nowDate +") AS sm" +
    "     LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_type = 'sm_code_type'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   ) AS v_sm_code_type ON sm.sm_code_type = v_sm_code_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_type = 'sm_is_more_box'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   ) AS v_sm_is_more_box ON sm.sm_is_more_box = v_sm_is_more_box.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_type = 'sm_is_claim'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   ) AS v_sm_is_claim ON sm.sm_is_claim = v_sm_is_claim.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_age_detection'" +
    "   ) AS v_sm_age_detection ON sm.sm_age_detection = v_sm_age_detection.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_volume_type'" +
    "   ) AS v_sm_volume_type ON sm.sm_volume_type = v_sm_volume_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_weight_type'" +
    "   ) AS v_sm_weight_type ON sm.sm_weight_type = v_sm_weight_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sfp_label_type'" +
    "   ) AS v_sfp_label_type ON sm.sfp_label_type = v_sfp_label_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'life_gate'" +
    "   ) AS v_life_gate ON sm.life_gate = v_life_gate.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_truck'" +
    "   ) AS v_sm_truck ON sm.sm_truck = v_sm_truck.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_volume'" +
    "   ) AS v_sm_is_volume ON sm.sm_is_volume = v_sm_is_volume.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_charge'" +
    "   ) AS v_sm_is_charge ON sm.sm_is_charge = v_sm_is_charge.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_validate_remote'" +
    "   ) AS v_sm_is_validate_remote ON sm.sm_is_validate_remote = v_sm_is_validate_remote.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_tracking'" +
    "   ) AS v_sm_is_tracking ON sm.sm_is_tracking = v_sm_is_tracking.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_signature'" +
    "   ) AS v_sm_is_signature ON sm.sm_is_signature = v_sm_is_signature.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_insurance'" +
    "   ) AS v_sm_is_insurance ON sm.sm_is_insurance = v_sm_is_insurance.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_type'" +
    "   ) AS v_sm_type ON sm.sm_type = v_sm_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_class_code'" +
    "   ) AS v_sm_class_code ON sm.sm_class_code = v_sm_class_code.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_status'" +
    "   ) AS v_sm_status ON sm.sm_status = v_sm_status.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'sm_allow_letter'" +
    "   ) AS v_sm_allow_letter ON sm.sm_allow_letter = v_sm_allow_letter.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9004'" +
    "   AND vl_datasource_table = 'gc_wms_shipping_method'" +
    "   AND vl_type = 'pre_generate_label'" +
    "   ) AS v_pre_generate_label ON sm.pre_generate_label = v_pre_generate_label.vl_value"
  private val zy_wms="select " +
    " sm.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " sm.datasource_num_id AS datasource_num_id," +
    " sm.data_flag AS data_flag," +
    " sm.integration_id AS integration_id," +
    " sm.created_on_dt AS created_on_dt," +
    " sm.changed_on_dt AS changed_on_dt," +
    " cast(CONCAT(sm.datasource_num_id, sm.warehouse_id) as bigint) AS warehouse_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.sc_id) as bigint) AS sc_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.st_id) as bigint) AS st_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.sct_id) as bigint) AS sct_key, " +
    " cast(CONCAT(sm.datasource_num_id, sm.sm_transfer_warehouse_id) as bigint) AS sm_transfer_warehouse_key," +
    " sm.sm_id AS sm_id," +
    " sm.sm_code AS sm_code," +
    " sm.sm_short_name AS sm_short_name," +
    " sm.sm_name_cn AS sm_name_cn," +
    " sm.sm_name AS sm_name," +
    " sm.pg_code AS sm_pg_code," +
    " sm.sm_mp_fee AS sm_mp_fee," +
    " sm.sm_reg_fee AS sm_reg_fee," +
    " sm.sm_addons AS sm_addons," +
    " sm.sm_type AS sm_type," +
    " nvl(v_sm_type.vl_bi_name, sm.sm_type) AS sm_type_val," +
    " sm.sm_baf AS sm_baf," +
    " sm.sm_discount AS sm_discount," +
    " sm.sm_delivery_time_min AS sm_delivery_time_min," +
    " sm.sm_delivery_time_max AS sm_delivery_time_max," +
    " sm.sm_delivery_time_avg AS sm_delivery_time_avg," +
    " sm.sm_is_volume AS sm_is_volume," +
    " nvl(v_sm_is_volume.vl_bi_name, sm.sm_is_volume) AS sm_is_volume_val," +
    " sm.sm_is_charge AS sm_is_charge," +
    " nvl(v_sm_is_charge.vl_bi_name, sm.sm_is_charge) AS sm_is_charge_val," +
    " sm.sm_vol_rate  AS sm_vol_rate," +
    " sm.sm_status AS sm_status," +
    " nvl(v_sm_status.vl_bi_name, sm.sm_status) AS sm_status_val," +
    " sm.sm_class_code AS sm_class_code," +
    " nvl(v_sm_class_code.vl_bi_name, sm.sm_class_code) AS sm_class_code_val," +
    " sm.sm_logo AS sm_logo," +
    " sm.sm_return_recipient AS sm_return_recipient," +
    " sm.sm_return_address AS sm_return_address," +
    " sm.sm_discount_min AS sm_discount_min," +
    " sm.sm_mp_fee_min AS sm_mp_fee_min," +
    " sm.sm_reg_fee_min AS sm_reg_fee_min," +
    " sm.sm_limit_volume AS sm_limit_volume," +
    " sm.sm_limit_weight AS sm_limit_weight," +
    " sm.sm_sort AS sm_sort," +
    " sm.sm_is_tracking AS sm_is_tracking," +
    " nvl(v_sm_is_tracking.vl_bi_name, sm.sm_is_tracking) AS sm_is_tracking_val," +
    " sm.sm_is_signature AS sm_is_signature," +
    " nvl(v_sm_is_signature.vl_bi_name, sm.sm_is_signature) AS sm_is_signature_val," +
    " sm.sm_is_insurance AS sm_is_insurance," +
    " nvl(v_sm_is_insurance.vl_bi_name, sm.sm_is_insurance) AS sm_is_insurance_val," +
    " sm.sm_is_validate_remote AS sm_is_validate_remote," +
    " nvl(v_sm_is_validate_remote.vl_bi_name, sm.sm_is_validate_remote) AS sm_is_validate_remote_val," +
    " sm.warehouse_id AS sm_warehouse_id," +
    " sm.sm_calc_type AS sm_calc_type," +
    " sm.sm_fee_type AS sm_fee_type," +
    " sm.sc_id AS sm_sc_id," +
    " sm.st_id AS sm_st_id," +
    " sm.sm_carrier_number AS sm_carrier_number," +
    " sm.sm_allow_letter AS sm_allow_letter," +
    " nvl(v_sm_allow_letter.vl_bi_name, sm.sm_allow_letter) AS sm_allow_letter_val," +
    " sm.sct_id AS sm_sct_id," +
    " sm.sm_update_time AS sm_update_time," +
    " sm.pre_generate_label AS sm_pre_generate_label," +
    " nvl(v_pre_generate_label.vl_bi_name, sm.pre_generate_label) AS sm_pre_generate_label_val," +
    " sm.sm_code_type AS sm_code_type," +
    " nvl(v_sm_code_type.vl_bi_name, sm.sm_code_type) AS sm_code_type_val," +
    " sm.sm_is_more_box AS sm_is_more_box," +
    " nvl(v_sm_is_more_box.vl_bi_name, sm.sm_is_more_box) AS sm_is_more_box_val," +
    " sm.sm_return_type AS sm_return_type," +
    " sm.sm_attribute AS sm_attribute," +
    " nvl(v_sm_attribute.vl_bi_name, sm.sm_attribute) AS sm_attribute_val," +
    " sm.sm_is_claim  AS sm_is_claim," +
    " nvl(v_sm_is_claim.vl_bi_name, sm.sm_is_claim) AS sm_is_claim_val," +
    " sm.sm_transfer_warehouse_id AS sm_transfer_warehouse_id," +
    " sm.sm_age_detection  AS sm_age_detection," +
    " nvl(v_sm_age_detection.vl_bi_name, sm.sm_age_detection) AS sm_age_detection_val," +
    " null AS sm_recommend_platform," +
    " null AS sm_volume_unit," +
    " null AS sm_volume_type," +
    " null AS sm_volume_type_val," +
    " null AS sm_volume_value," +
    " null AS sm_weight_unit," +
    " null AS sm_weight_type," +
    " null AS sm_weight_type_val," +
    " null AS sm_weight_value," +
    " null AS sm_claim_desc," +
    " null AS sm_sfp_label_type," +
    " null AS sm_sfp_label_type_val," +
    " null AS sm_life_gate," +
    " null AS sm_life_gate_val," +
    " null AS sm_truck," +
    " null AS sm_truck_val" +
    " from (SELECT * from ODS.zy_wms_shipping_method as gos where gos.day="+ nowDate +") AS sm" +
    "     LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'sm_code_type'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   ) AS v_sm_code_type ON sm.sm_code_type = v_sm_code_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'sm_is_more_box'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   ) AS v_sm_is_more_box ON sm.sm_is_more_box = v_sm_is_more_box.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_type = 'sm_is_claim'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   ) AS v_sm_is_claim ON sm.sm_is_claim = v_sm_is_claim.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_age_detection'" +
    "   ) AS v_sm_age_detection ON sm.sm_age_detection = v_sm_age_detection.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_volume'" +
    "   ) AS v_sm_is_volume ON sm.sm_is_volume = v_sm_is_volume.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_charge'" +
    "   ) AS v_sm_is_charge ON sm.sm_is_charge = v_sm_is_charge.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_validate_remote'" +
    "   ) AS v_sm_is_validate_remote ON sm.sm_is_validate_remote = v_sm_is_validate_remote.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_tracking'" +
    "   ) AS v_sm_is_tracking ON sm.sm_is_tracking = v_sm_is_tracking.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_signature'" +
    "   ) AS v_sm_is_signature ON sm.sm_is_signature = v_sm_is_signature.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_is_insurance'" +
    "   ) AS v_sm_is_insurance ON sm.sm_is_insurance = v_sm_is_insurance.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_type'" +
    "   ) AS v_sm_type ON sm.sm_type = v_sm_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_class_code'" +
    "   ) AS v_sm_class_code ON sm.sm_class_code = v_sm_class_code.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_status'" +
    "   ) AS v_sm_status ON sm.sm_status = v_sm_status.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_allow_letter'" +
    "   ) AS v_sm_allow_letter ON sm.sm_allow_letter = v_sm_allow_letter.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'pre_generate_label'" +
    "   ) AS v_pre_generate_label ON sm.pre_generate_label = v_pre_generate_label.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9022'" +
    "   AND vl_datasource_table = 'zy_wms_shipping_method'" +
    "   AND vl_type = 'sm_attribute'" +
    "   ) AS v_sm_attribute ON sm.sm_attribute = v_sm_attribute.vl_value"
  private val gc_oms="select " +
    " sm.row_wid AS row_wid," +
    " from_unixtime(unix_timestamp(current_date()),'yyyyMMdd') AS etl_proc_wid," +
    " CURRENT_TIMESTAMP() AS w_insert_dt," +
    " CURRENT_TIMESTAMP() AS w_update_dt," +
    " sm.datasource_num_id AS datasource_num_id," +
    " sm.data_flag AS data_flag," +
    " sm.integration_id AS integration_id," +
    " sm.created_on_dt AS created_on_dt," +
    " sm.changed_on_dt AS changed_on_dt," +
    " cast(CONCAT(sm.datasource_num_id, sm.warehouse_id) as bigint) AS warehouse_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.sc_id) as bigint) AS sc_key," +
    " cast(CONCAT(sm.datasource_num_id, sm.st_id) as bigint) AS st_key," +
    " null AS sct_key, " +
    " cast(CONCAT(sm.datasource_num_id, sm.sm_transfer_warehouse_id) as bigint) AS sm_transfer_warehouse_key," +
    " sm.sm_id AS sm_id," +
    " sm.sm_code AS sm_code," +
    " sm.sm_short_name AS sm_short_name," +
    " sm.sm_name_cn AS sm_name_cn," +
    " sm.sm_name AS sm_name," +
    " null AS sm_pg_code," +
    " sm.sm_mp_fee AS sm_mp_fee," +
    " sm.sm_reg_fee AS sm_reg_fee," +
    " sm.sm_addons AS sm_addons," +
    " sm.sm_type AS sm_type," +
    " nvl(v_sm_type.vl_bi_name, sm.sm_type) AS sm_type_val," +
    " sm.sm_baf AS sm_baf," +
    " sm.sm_discount AS sm_discount," +
    " sm.sm_delivery_time_min AS sm_delivery_time_min," +
    " sm.sm_delivery_time_max AS sm_delivery_time_max," +
    " sm.sm_delivery_time_avg AS sm_delivery_time_avg," +
    " sm.sm_is_volume AS sm_is_volume," +
    " nvl(v_sm_is_volume.vl_bi_name, sm.sm_is_volume) AS sm_is_volume_val," +
    " sm.sm_is_charge AS sm_is_charge," +
    " nvl(v_sm_is_charge.vl_bi_name, sm.sm_is_charge) AS sm_is_charge_val," +
    " sm.sm_vol_rate  AS sm_vol_rate," +
    " sm.sm_status AS sm_status," +
    " nvl(v_sm_status.vl_bi_name, sm.sm_status) AS sm_status_val," +
    " sm.sm_class_code AS sm_class_code," +
    " nvl(v_sm_class_code.vl_bi_name, sm.sm_class_code) AS sm_class_code_val," +
    " sm.sm_logo AS sm_logo," +
    " sm.sm_return_recipient AS sm_return_recipient," +
    " sm.sm_return_address AS sm_return_address," +
    " sm.sm_discount_min AS sm_discount_min," +
    " sm.sm_mp_fee_min AS sm_mp_fee_min," +
    " sm.sm_reg_fee_min AS sm_reg_fee_min," +
    " sm.sm_limit_volume AS sm_limit_volume," +
    " sm.sm_limit_weight AS sm_limit_weight," +
    " sm.sm_sort AS sm_sort," +
    " sm.sm_is_tracking AS sm_is_tracking," +
    " nvl(v_sm_is_tracking.vl_bi_name, sm.sm_is_tracking) AS sm_is_tracking_val," +
    " sm.sm_is_signature AS sm_is_signature," +
    " nvl(v_sm_is_signature.vl_bi_name, sm.sm_is_signature) AS sm_is_signature_val," +
    " sm.sm_is_insurance AS sm_is_insurance," +
    " nvl(v_sm_is_insurance.vl_bi_name, sm.sm_is_insurance) AS sm_is_insurance_val," +
    " sm.sm_is_validate_remote AS sm_is_validate_remote," +
    " nvl(v_sm_is_validate_remote.vl_bi_name, sm.sm_is_validate_remote) AS sm_is_validate_remote_val," +
    " sm.warehouse_id AS sm_warehouse_id," +
    " sm.sm_calc_type AS sm_calc_type," +
    " sm.sm_fee_type AS sm_fee_type," +
    " sm.sc_id AS sm_sc_id," +
    " sm.st_id AS sm_st_id," +
    " sm.sm_carrier_number AS sm_carrier_number," +
    " null AS sm_allow_letter," +
    " null AS sm_allow_letter_val," +
    " null AS sm_sct_id," +
    " null AS sm_update_time," +
    " null AS sm_pre_generate_label," +
    " null AS sm_pre_generate_label_val," +
    " sm.sm_code_type AS sm_code_type," +
    " nvl(v_sm_code_type.vl_bi_name, sm.sm_code_type) AS sm_code_type_val," +
    " sm.sm_is_more_box AS sm_is_more_box," +
    " nvl(v_sm_is_more_box.vl_bi_name, sm.sm_is_more_box) AS sm_is_more_box_val," +
    " sm.sm_return_type AS sm_return_type," +
    " null AS sm_attribute," +
    " null AS sm_attribute_val," +
    " null AS sm_is_claim," +
    " null AS sm_is_claim_val," +
    " sm.sm_transfer_warehouse_id AS sm_transfer_warehouse_id," +
    " sm.sm_age_detection  AS sm_age_detection," +
    " nvl(v_sm_age_detection.vl_bi_name, sm.sm_age_detection) AS sm_age_detection_val," +
    " sm.sm_recommend_platform AS sm_recommend_platform," +
    " null AS sm_volume_unit," +
    " null AS sm_volume_type," +
    " null AS sm_volume_type_val," +
    " null AS sm_volume_value," +
    " null AS sm_weight_unit," +
    " null AS sm_weight_type," +
    " null AS sm_weight_type_val," +
    " null AS sm_weight_value," +
    " null AS sm_claim_desc," +
    " null AS sm_sfp_label_type," +
    " null AS sm_sfp_label_type_val," +
    " sm.life_gate AS sm_life_gate," +
    " nvl(v_life_gate.vl_bi_name, sm.life_gate) AS sm_life_gate_val," +
    " sm.sm_truck AS sm_truck," +
    " nvl(v_sm_truck.vl_bi_name, sm.sm_truck) AS sm_truck_val" +
    " from (SELECT * from ODS.gc_oms_shipping_method as gos where gos.day="+ nowDate +") AS sm" +
    "     LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_type = 'sm_code_type'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   ) AS v_sm_code_type ON sm.sm_code_type = v_sm_code_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_type = 'sm_is_more_box'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   ) AS v_sm_is_more_box ON sm.sm_is_more_box = v_sm_is_more_box.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_age_detection'" +
    "   ) AS v_sm_age_detection ON sm.sm_age_detection = v_sm_age_detection.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'life_gate'" +
    "   ) AS v_life_gate ON sm.life_gate = v_life_gate.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_truck'" +
    "   ) AS v_sm_truck ON sm.sm_truck = v_sm_truck.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_is_volume'" +
    "   ) AS v_sm_is_volume ON sm.sm_is_volume = v_sm_is_volume.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_is_charge'" +
    "   ) AS v_sm_is_charge ON sm.sm_is_charge = v_sm_is_charge.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_is_validate_remote'" +
    "   ) AS v_sm_is_validate_remote ON sm.sm_is_validate_remote = v_sm_is_validate_remote.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_is_tracking'" +
    "   ) AS v_sm_is_tracking ON sm.sm_is_tracking = v_sm_is_tracking.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_is_signature'" +
    "   ) AS v_sm_is_signature ON sm.sm_is_signature = v_sm_is_signature.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_is_insurance'" +
    "   ) AS v_sm_is_insurance ON sm.sm_is_insurance = v_sm_is_insurance.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_type'" +
    "   ) AS v_sm_type ON sm.sm_type = v_sm_type.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_class_code'" +
    "   ) AS v_sm_class_code ON sm.sm_class_code = v_sm_class_code.vl_value" +
    " LEFT JOIN (" +
    "   SELECT * FROM dw.par_val_list" +
    "   WHERE datasource_num_id = '9001'" +
    "   AND vl_datasource_table = 'gc_oms_shipping_method'" +
    "   AND vl_type = 'sm_status'" +
    "   ) AS v_sm_status ON sm.sm_status = v_sm_status.vl_value"


  def main(args: Array[String]): Unit = {
    val sqlArray: Array[String] = Array(gc_wms,zy_wms,gc_oms).map(_.replaceAll("dw.par_val_list", Dw_par_val_list_cache.TEMP_PAR_VAL_LIST_NAME))
    Dw_dim_common.getRunCode_full(task, tableName, sqlArray, Array(SystemCodeUtil.GC_WMS,SystemCodeUtil.ZY_WMS,SystemCodeUtil.GC_OMS))
  }
}
