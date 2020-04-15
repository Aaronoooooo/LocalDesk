package com.zongteng.ztetl.etl.dm.receiving

import com.zongteng.ztetl.common.DmCommon

object DmOnwayFull {
  //任务名称(一般同类名)
  private val task = "Dm_onway_full"

  //dw层类名
  private val tableName = "dm_onway"

  //要执行的sql语句
  private val dm_onway="SELECT" +
    "  fr.receiving_id AS receiving_id," +
    "  frd.rd_id AS rd_id," +
    "  sm.sm_name_cn AS sm_name_cn," +
    "  frd.product_key AS product_key," +
    "  pr.product_title AS product_title," +
    "  frd.rd_product_barcode AS rd_product_barcode," +
    "  c.customer_name AS customer_name," +
    "  c.customer_company_name AS customer_company_name," +
    "  fr.receiving_customer_code AS receiving_customer_code," +
    "  wh.warehouse_desc AS warehouse_desc," +
    "  whm.warehouse_desc AS to_warehouse_desc," +
    "  whm.warehouse_code AS warehouse_code," +
    "  fr.receiving_code AS receiving_code," +
    "  concat( '\\'', fr.receiving_tracking_number ) AS receiving_tracking_number," +
    "  fr.receiving_container_number AS receiving_container_number," +
    "  fr.receiving_reference_no AS receiving_reference_no," +
    "  fcb.rb_box_no AS rb_box_no," +
    "  nvl ( fcbd.rbd_quantity, frd.rd_receiving_qty ) AS rd_receiving_qty," +
    "  fr.receiving_type AS receiving_type," +
    "  fr.receiving_status AS receiving_status," +
    "  fr.receiving_sm_code AS receiving_sm_code," +
    "  fr.receiving_shipping_type AS receiving_shipping_type," +
    "  fr.receiving_add_time AS receiving_add_time," +
    "  fr.receiving_income_type AS receiving_income_type," +
    "  CASE " +
    "  WHEN fr.receiving_expected_date is not null THEN" +
    "  to_date(fr.receiving_expected_date)  " +
    "  WHEN (whm.warehouse_desc LIKE '%美%' " +
    "  or whm.warehouse_desc LIKE '%英%' " +
    "  or whm.warehouse_desc LIKE '%澳%'" +
    "  or whm.warehouse_desc IN ( '捷克仓库', '法国仓库', '意大利仓库', '西班牙仓库' )" +
    "  )" +
    "  AND fr.receiving_shipping_type_val = '快递' THEN" +
    "  date_add( fr.receiving_add_time, 7 )  " +
    "  WHEN (whm.warehouse_desc LIKE '%美%' " +
    "  or whm.warehouse_desc LIKE '%英%'" +
    "  or whm.warehouse_desc LIKE '%澳%'" +
    "  or whm.warehouse_desc IN ( '捷克仓库', '法国仓库', '意大利仓库', '西班牙仓库' )" +
    "  )" +
    "  AND fr.receiving_shipping_type_val = '空运' THEN" +
    "  date_add( fr.receiving_add_time, 10 ) " +
    "  WHEN (whm.warehouse_desc LIKE '%美东%' " +
    "  OR whm.warehouse_desc LIKE '%美南%' " +
    "  ) " +
    "  AND fr.receiving_shipping_type_val LIKE '%海运%' THEN" +
    "  date_add( fr.receiving_add_time, 50 ) " +
    "  WHEN whm.warehouse_desc LIKE '%美西%' " +
    "  AND fr.receiving_shipping_type_val LIKE '%海运%' THEN" +
    "  date_add( fr.receiving_add_time, 35 ) " +
    "  WHEN (whm.warehouse_desc LIKE '%英%' " +
    "  or whm.warehouse_desc LIKE '%澳%' " +
    "  or whm.warehouse_desc IN ( '捷克仓库', '法国仓库', '意大利仓库', '西班牙仓库' )" +
    "  ) " +
    "  AND fr.receiving_shipping_type_val LIKE '%海运%' THEN" +
    "  date_add( fr.receiving_add_time, 55 ) " +
    "  " +
    "  WHEN (whm.warehouse_desc LIKE '%美东%' " +
    "  OR whm.warehouse_desc LIKE '%美南%' " +
    "  )  " +
    "  AND fr.receiving_shipping_type_val = '铁运' THEN" +
    "  date_add( fr.receiving_add_time, 50 ) " +
    "  WHEN whm.warehouse_desc LIKE '%美西%' " +
    "  AND fr.receiving_shipping_type_val = '铁运' THEN" +
    "  date_add( fr.receiving_add_time, 35 ) " +
    "  WHEN (whm.warehouse_desc LIKE '%英%' " +
    "  or whm.warehouse_desc LIKE '%澳%'" +
    "  or whm.warehouse_desc IN ( '捷克仓库', '法国仓库', '意大利仓库', '西班牙仓库' ) " +
    "  )" +
    "  AND fr.receiving_shipping_type_val = '铁运' THEN" +
    "  date_add( fr.receiving_add_time, 40 ) ELSE NULL " +
    "  END AS receiving_expected_date," +
    "  fcb.rb_update_time AS rb_update_time," +
    "  CASE " +
    "  WHEN fr.receiving_is_fba = 1 THEN" +
    "  '是' ELSE '否' " +
    "  END AS receiving_is_fba," +
    "  fr.receiving_create_type AS receiving_create_type," +
    "  frd.rd_fba_product_code AS rd_fba_product_code," +
    "  CASE " +
    "  WHEN frd.rd_id IS NOT NULL THEN" +
    "  row_number ( ) over (" +
    "  PARTITION BY fr.receiving_id," +
    "  frd.rd_id," +
    "  frd.product_key," +
    "  fr.customer_key," +
    "  fr.receiving_code," +
    "  fcb.rb_box_no " +
    "  ORDER BY" +
    "  fr.receiving_id," +
    "  frd.rd_id," +
    "  frd.product_key," +
    "  fr.customer_key " +
    "  ) ELSE 1 " +
    "  END AS px," +
    "  CASE " +
    "  WHEN pr.product_receive_status_val = '新产品' THEN" +
    "  '新品' ELSE '旧品' " +
    "  END AS product_receive_status," +
    "  pr.product_dm_length AS product_length," +
    "  pr.product_dm_width AS product_width," +
    "  pr.product_dm_height AS product_height," +
    "  (" +
    "  pr.product_dm_length * " +
    "  pr.product_dm_width * " +
    "  pr.product_dm_height * nvl ( fcbd.rbd_quantity, frd.rd_receiving_qty ) " +
    "  ) / 1000000 AS rbd_quantity," +
    "  pr.product_dm_weight * nvl ( fcbd.rbd_quantity, frd.rd_receiving_qty ) AS product_weight," +
    "  tem.to_dept AS to_dept," +
    "  em.employee_name AS employee_name," +
    "  CASE" +
    "  WHEN fr.datasource_num_id = '9004' THEN" +
    "  '谷仓' " +
    "  WHEN fr.datasource_num_id = '9022' THEN" +
    "  '中邮' ELSE fr.datasource_num_id " +
    "  END AS datasource_num_id," +
    "  concat( nvl ( buaccount.account, '' ), '|', nvl ( buaccount2.account, '' ), '|', buaccount3.account ) AS account," +
    "  CURRENT_TIMESTAMP ( ) AS update_time" +
    " FROM" +
    "  ( SELECT * FROM dw.fact_receiving WHERE datasource_num_id IN ( '9004', '9022' ) ) fr" +
    "  LEFT JOIN ( SELECT * FROM dw.fact_receiving_detail WHERE datasource_num_id IN ( '9004', '9022' ) ) frd ON fr.receiving_id = frd.rd_receiving_id " +
    "  AND fr.datasource_num_id = frd.datasource_num_id" +
    "  LEFT JOIN dw.dim_warehouse wh ON fr.warehouse_key = wh.row_wid" +
    "  LEFT JOIN dw.dim_warehouse whm ON fr.to_warehouse_key = whm.row_wid" +
    "  LEFT JOIN dw.dim_customer c ON fr.customer_key = c.row_wid" +
    "  LEFT JOIN dw.dim_shipping_method sm ON fr.sm_key = sm.row_wid" +
    "  LEFT JOIN dw.dim_product pr ON frd.product_key = pr.row_wid" +
    "  LEFT JOIN ( SELECT * FROM dw.fact_receiving_box_detail WHERE datasource_num_id IN ( '9004', '9022' ) ) fcbd ON frd.rd_receiving_id = fcbd.rbd_receiving_id " +
    "  AND frd.datasource_num_id = fcbd.datasource_num_id " +
    "  AND frd.product_key = fcbd.product_key" +
    "  LEFT JOIN ( SELECT * FROM dw.fact_receiving_box WHERE datasource_num_id IN ( '9004', '9022' ) ) fcb ON fcb.rb_id = fcbd.rbd_rb_id " +
    "  AND fcb.datasource_num_id = fcbd.datasource_num_id" +
    "  LEFT JOIN dw.dim_employee em ON c.customer_saler_user_id = em.employee_id " +
    "  AND em.datasource_num_id = c.datasource_num_id" +
    "  LEFT JOIN dw.tmp_employee_to_dept tem ON em.employee_id = tem.employee_id " +
    "  AND tem.datasource_num_id = em.datasource_num_id" +
    "  LEFT JOIN dw.tmp_tableau_privilege buaccount ON tem.to_dept = buaccount.NAME " +
    "  AND buaccount.type = '业务'" +
    "  LEFT JOIN dw.tmp_tableau_privilege buaccount2 ON whm.warehouse_code = buaccount2.NAME " +
    "  AND buaccount2.Type = '仓库'" +
    "  LEFT JOIN ( SELECT account FROM dw.tmp_tableau_privilege WHERE type = 'all' ) buaccount3 ON 1 = 1 " +
    " WHERE" +
    "  fr.receiving_status_val in('在途','草稿(待审核)')" +
    "  and not exists(select 1 from dw.tmp_zt136_wmstestcustomerlist test where test.customer_id = fr.receiving_customer_id)" +
    "  and whm.row_wid not in (90015,90016,90045,90046,90225,90226)" +
    "  and whm.warehouse_status_val='可用'" +
    "  and fr.receiving_code not like 'RSO%'"

  def main(args: Array[String]): Unit = {
    DmCommon.getRunCodeFull(task,tableName,Array(dm_onway))
  }
}
