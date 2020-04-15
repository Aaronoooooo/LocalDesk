package com.zongteng.ztetl.util.stream

object AllowMysqlTable {

    // 允许那些表可以添加进来（mysql的表）
    val tables = Array[String](

      // 入库单（27张表）
      "gc_wms_gc_receiving_box",
      "gc_wms_receiving_box",
      "zy_wms_receiving_box",
      "gc_oms_receiving",
      "gc_wms_gc_receiving",
      "gc_wms_receiving",
      "zy_wms_receiving",
      "gc_wms_gc_receiving_batch",
      "gc_wms_receiving_detail_batch",
      "zy_wms_receiving_detail_batch",
      "gc_wms_gc_receiving_log",
      "gc_wms_receiving_log",
      "zy_wms_receiving_log",
      "gc_wms_putaway",
      "zy_wms_putaway",
      "gc_wms_gc_putaway_detail",
      "gc_wms_putaway_detail",
      "zy_wms_putaway_detail",
      "gc_oms_receiving_detail",
      "gc_wms_gc_receiving_detail",
      "gc_wms_receiving_detail",
      "zy_wms_receiving_detail",
      "gc_wms_gc_receiving_box_detail",
      "gc_wms_receiving_box_detail",
      "zy_wms_receiving_box_detail",
      "gc_wms_quality_control",
      "zy_wms_quality_control",
      //订单(orders,64张表)
      "gc_wms_order_log",
      "zy_wms_order_log",
      "gc_wms_order_operation_time",
      "zy_wms_order_operation_time",
      "gc_wms_ship_order",
      "zy_wms_ship_order",
      "gc_wms_order_address_book",
      "zy_wms_order_address_book",
      "gc_owms_usea_order_address_book",
      "gc_owms_uswe_order_address_book",
      "gc_owms_au_order_address_book",
      "gc_owms_cz_order_address_book",
      "gc_owms_de_order_address_book",
      "gc_owms_es_order_address_book",
      "gc_owms_frvi_order_address_book",
      "gc_owms_it_order_address_book",
      "gc_owms_jp_order_address_book",
      "gc_owms_uk_order_address_book",
      "gc_owms_ukob_order_address_book",
      "gc_owms_usnb_order_address_book",
      "gc_owms_usot_order_address_book",
      "gc_owms_ussc_order_address_book",
      "zy_owms_au_order_address_book",
      "zy_owms_cz_order_address_book",
      "zy_owms_de_order_address_book",
      "zy_owms_ru_order_address_book",
      "zy_owms_uk_order_address_book",
      "zy_owms_usea_order_address_book",
      "zy_owms_uswe_order_address_book",
      "zy_owms_ussc_order_address_book",
      "gc_wms_order_product",
      "zy_wms_order_product",
      "gc_wms_order_physical_relation",
      "zy_wms_order_product_physical_relation",
      "gc_owms_au_orders",
      "gc_owms_cz_orders",
      "gc_owms_de_orders",
      "gc_owms_es_orders",
      "gc_owms_frvi_orders",
      "gc_owms_it_orders",
      "gc_owms_jp_orders",
      "gc_owms_uk_orders",
      "gc_owms_ukob_orders",
      "gc_owms_usea_orders",
      "gc_owms_uswe_orders",
      "gc_owms_usnb_orders",
      "gc_owms_usot_orders",
      "gc_owms_ussc_orders",
      "zy_owms_au_orders",
      "zy_owms_cz_orders",
      "zy_owms_de_orders",
      "zy_owms_ru_orders",
      "zy_owms_uk_orders",
      "zy_owms_usea_orders",
      "zy_owms_uswe_orders",
      "zy_owms_ussc_orders",
      "gc_wms_orders",
      "zy_wms_orders",
      "gc_oms_orders",
      "gc_wms_return_orders",
      "zy_wms_return_orders",
      "gc_oms_return_orders",
      "gc_wms_after_sales_return_orders",
      "zy_wms_after_sales_return_orders",
      //出库(picking 50张表)
      "gc_owms_au_picking_physical_relation",
      "gc_owms_cz_picking_physical_relation",
      "gc_owms_de_picking_physical_relation",
      "gc_owms_es_picking_physical_relation",
      "gc_owms_it_picking_physical_relation",
      "gc_owms_jp_picking_physical_relation",
      "gc_owms_uk_picking_physical_relation",
      "gc_owms_frvi_picking_physical_relation",
      "gc_owms_ukob_picking_physical_relation",
      "gc_owms_usnb_picking_physical_relation",
      "gc_owms_usot_picking_physical_relation",
      "gc_owms_ussc_picking_physical_relation",
      "gc_owms_usea_picking_physical_relation",
      "gc_owms_uswe_picking_physical_relation",
      "gc_owms_au_wellen_area",
      "gc_owms_cz_wellen_area",
      "gc_owms_de_wellen_area",
      "gc_owms_es_wellen_area",
      "gc_owms_it_wellen_area",
      "gc_owms_jp_wellen_area",
      "gc_owms_uk_wellen_area",
      "gc_owms_frvi_wellen_area",
      "gc_owms_ukob_wellen_area",
      "gc_owms_usnb_wellen_area",
      "gc_owms_usot_wellen_area",
      "gc_owms_ussc_wellen_area",
      "gc_owms_usea_wellen_area",
      "gc_owms_uswe_wellen_area",
      "zy_owms_au_wellen_area",
      "zy_owms_cz_wellen_area",
      "zy_owms_de_wellen_area",
      "zy_owms_ru_wellen_area",
      "zy_owms_uk_wellen_area",
      "zy_owms_ussc_wellen_area",
      "zy_owms_usea_wellen_area",
      "zy_owms_uswe_wellen_area",
      "gc_owms_au_wellen_log",
      "gc_owms_cz_wellen_log",
      "gc_owms_de_wellen_log",
      "gc_owms_es_wellen_log",
      "gc_owms_it_wellen_log",
      "gc_owms_jp_wellen_log",
      "gc_owms_uk_wellen_log",
      "gc_owms_frvi_wellen_log",
      "gc_owms_ukob_wellen_log",
      "gc_owms_usnb_wellen_log",
      "gc_owms_usot_wellen_log",
      "gc_owms_ussc_wellen_log",
      "gc_owms_usea_wellen_log",
      "gc_owms_uswe_wellen_log",
      //仓储(storage 15张表)
      "gc_wms_inventory_batch",
      "zy_wms_inventory_batch",
      "gc_wms_product_inventory",
      "zy_wms_product_inventory",
      "gc_wms_take_stock",
      "zy_wms_take_stock",
      "gc_wms_take_stock_assignment",
      "zy_wms_take_stock_assignment",
      "gc_wms_take_stock_item",
      "zy_wms_take_stock_item",
      "gc_wms_flow_volume",
      "gc_wms_inventory_difference",
      "gc_wms_inventory_difference_detail",
      "gc_wms_inventory_batch_log",
      "zy_wms_inventory_batch_log"
    )

  val table = {
    pickingTables.union(tables).distinct
  }



  def pickingTables = {
    Array(      "gc_wms_advance_picking_detail",
      "zy_wms_advance_picking_detail",
      "gc_owms_au_advance_picking_detail",
      "gc_owms_cz_advance_picking_detail",
      "gc_owms_de_advance_picking_detail",
      "gc_owms_es_advance_picking_detail",
      "gc_owms_frvi_advance_picking_detail",
      "gc_owms_it_advance_picking_detail",
      "gc_owms_jp_advance_picking_detail",
      "gc_owms_uk_advance_picking_detail",
      "gc_owms_ukob_advance_picking_detail",
      "gc_owms_usea_advance_picking_detail",
      "gc_owms_uswe_advance_picking_detail",
      "gc_owms_usnb_advance_picking_detail",
      "gc_owms_usot_advance_picking_detail",
      "gc_owms_ussc_advance_picking_detail",
      "zy_owms_au_advance_picking_detail",
      "zy_owms_cz_advance_picking_detail",
      "zy_owms_de_advance_picking_detail",
      "zy_owms_ru_advance_picking_detail",
      "zy_owms_uk_advance_picking_detail",
      "zy_owms_usea_advance_picking_detail",
      "zy_owms_uswe_advance_picking_detail",
      "zy_owms_ussc_advance_picking_detail",
      "gc_wms_picking",
      "zy_wms_picking",
      "gc_owms_usea_picking",
      "gc_owms_uswe_picking",
      "gc_owms_au_picking",
      "gc_owms_cz_picking",
      "gc_owms_de_picking",
      "gc_owms_es_picking",
      "gc_owms_frvi_picking",
      "gc_owms_it_picking",
      "gc_owms_jp_picking",
      "gc_owms_uk_picking",
      "gc_owms_ukob_picking",
      "gc_owms_usnb_picking",
      "gc_owms_usot_picking",
      "gc_owms_ussc_picking",
      "zy_owms_au_picking",
      "zy_owms_cz_picking",
      "zy_owms_de_picking",
      "zy_owms_ru_picking",
      "zy_owms_uk_picking",
      "zy_owms_usea_picking",
      "zy_owms_uswe_picking",
      "zy_owms_ussc_picking",
      "gc_wms_picking_detail",
      "zy_wms_picking_detail",
      "gc_owms_usea_picking_detail",
      "gc_owms_uswe_picking_detail",
      "gc_owms_au_picking_detail",
      "gc_owms_cz_picking_detail",
      "gc_owms_de_picking_detail",
      "gc_owms_es_picking_detail",
      "gc_owms_frvi_picking_detail",
      "gc_owms_it_picking_detail",
      "gc_owms_jp_picking_detail",
      "gc_owms_uk_picking_detail",
      "gc_owms_ukob_picking_detail",
      "gc_owms_usnb_picking_detail",
      "gc_owms_usot_picking_detail",
      "gc_owms_ussc_picking_detail",
      "zy_owms_au_picking_detail",
      "zy_owms_cz_picking_detail",
      "zy_owms_de_picking_detail",
      "zy_owms_ru_picking_detail",
      "zy_owms_uk_picking_detail",
      "zy_owms_usea_picking_detail",
      "zy_owms_uswe_picking_detail",
      "zy_owms_ussc_picking_detail",
      "gc_owms_au_wellen_area",
      "gc_owms_cz_wellen_area",
      "gc_owms_de_wellen_area",
      "gc_owms_es_wellen_area",
      "gc_owms_it_wellen_area",
      "gc_owms_jp_wellen_area",
      "gc_owms_uk_wellen_area",
      "gc_owms_frvi_wellen_area",
      "gc_owms_ukob_wellen_area",
      "gc_owms_usnb_wellen_area",
      "gc_owms_usot_wellen_area",
      "gc_owms_ussc_wellen_area",
      "gc_owms_usea_wellen_area",
      "gc_owms_uswe_wellen_area",
      "zy_owms_au_wellen_area",
      "zy_owms_cz_wellen_area",
      "zy_owms_de_wellen_area",
      "zy_owms_ru_wellen_area",
      "zy_owms_uk_wellen_area",
      "zy_owms_ussc_wellen_area",
      "zy_owms_usea_wellen_area",
      "zy_owms_uswe_wellen_area",
      "gc_owms_au_wellen_log",
      "gc_owms_cz_wellen_log",
      "gc_owms_de_wellen_log",
      "gc_owms_es_wellen_log",
      "gc_owms_it_wellen_log",
      "gc_owms_jp_wellen_log",
      "gc_owms_uk_wellen_log",
      "gc_owms_frvi_wellen_log",
      "gc_owms_ukob_wellen_log",
      "gc_owms_usnb_wellen_log",
      "gc_owms_usot_wellen_log",
      "gc_owms_ussc_wellen_log",
      "gc_owms_usea_wellen_log",
      "gc_owms_uswe_wellen_log",
      "gc_owms_au_new_wellen_log",
      "gc_owms_cz_new_wellen_log",
      "gc_owms_de_new_wellen_log",
      "gc_owms_es_new_wellen_log",
      "gc_owms_it_new_wellen_log",
      "gc_owms_jp_new_wellen_log",
      "gc_owms_uk_new_wellen_log",
      "gc_owms_frvi_new_wellen_log",
      "gc_owms_ukob_new_wellen_log",
      "gc_owms_usnb_new_wellen_log",
      "gc_owms_usot_new_wellen_log",
      "gc_owms_ussc_new_wellen_log",
      "gc_owms_usea_new_wellen_log",
      "gc_owms_uswe_new_wellen_log",
      "gc_owms_au_wellen_rule",
      "gc_owms_cz_wellen_rule",
      "gc_owms_de_wellen_rule",
      "gc_owms_es_wellen_rule",
      "gc_owms_frvi_wellen_rule",
      "gc_owms_it_wellen_rule",
      "gc_owms_jp_wellen_rule",
      "gc_owms_uk_wellen_rule",
      "gc_owms_ukob_wellen_rule",
      "gc_owms_usea_wellen_rule",
      "gc_owms_uswe_wellen_rule",
      "gc_owms_usnb_wellen_rule",
      "gc_owms_usot_wellen_rule",
      "gc_owms_ussc_wellen_rule",
      "gc_owms_au_new_wellen_rule",
      "gc_owms_cz_new_wellen_rule",
      "gc_owms_de_new_wellen_rule",
      "gc_owms_es_new_wellen_rule",
      "gc_owms_frvi_new_wellen_rule",
      "gc_owms_it_new_wellen_rule",
      "gc_owms_jp_new_wellen_rule",
      "gc_owms_uk_new_wellen_rule",
      "gc_owms_ukob_new_wellen_rule",
      "gc_owms_usea_new_wellen_rule",
      "gc_owms_uswe_new_wellen_rule",
      "gc_owms_usnb_new_wellen_rule",
      "gc_owms_usot_new_wellen_rule",
      "gc_owms_ussc_new_wellen_rule",
      "zy_owms_au_wellen_rule",
      "zy_owms_cz_wellen_rule",
      "zy_owms_de_wellen_rule",
      "zy_owms_ru_wellen_rule",
      "zy_owms_uk_wellen_rule",
      "zy_owms_usea_wellen_rule",
      "zy_owms_uswe_wellen_rule",
      "zy_owms_ussc_wellen_rule",
      "gc_owms_au_wellen_sc",
      "gc_owms_cz_wellen_sc",
      "gc_owms_de_wellen_sc",
      "gc_owms_es_wellen_sc",
      "gc_owms_frvi_wellen_sc",
      "gc_owms_it_wellen_sc",
      "gc_owms_jp_wellen_sc",
      "gc_owms_uk_wellen_sc",
      "gc_owms_ukob_wellen_sc",
      "gc_owms_usea_wellen_sc",
      "gc_owms_uswe_wellen_sc",
      "gc_owms_usnb_wellen_sc",
      "gc_owms_usot_wellen_sc",
      "gc_owms_ussc_wellen_sc",
      "zy_owms_au_wellen_sc",
      "zy_owms_cz_wellen_sc",
      "zy_owms_de_wellen_sc",
      "zy_owms_ru_wellen_sc",
      "zy_owms_uk_wellen_sc",
      "zy_owms_usea_wellen_sc",
      "zy_owms_uswe_wellen_sc",
      "zy_owms_ussc_wellen_sc",
      "gc_owms_au_picking_physical_relation",
      "gc_owms_cz_picking_physical_relation",
      "gc_owms_de_picking_physical_relation",
      "gc_owms_es_picking_physical_relation",
      "gc_owms_it_picking_physical_relation",
      "gc_owms_jp_picking_physical_relation",
      "gc_owms_uk_picking_physical_relation",
      "gc_owms_frvi_picking_physical_relation",
      "gc_owms_ukob_picking_physical_relation",
      "gc_owms_usnb_picking_physical_relation",
      "gc_owms_usot_picking_physical_relation",
      "gc_owms_ussc_picking_physical_relation",
      "gc_owms_usea_picking_physical_relation",
      "gc_owms_uswe_picking_physical_relation",
      "gc_wms_picking_mode",
      "zy_wms_picking_mode",
      "gc_owms_usea_picking_mode",
      "gc_owms_uswe_picking_mode",
      "gc_owms_au_picking_mode",
      "gc_owms_cz_picking_mode",
      "gc_owms_de_picking_mode",
      "gc_owms_es_picking_mode",
      "gc_owms_frvi_picking_mode",
      "gc_owms_it_picking_mode",
      "gc_owms_jp_picking_mode",
      "gc_owms_uk_picking_mode",
      "gc_owms_ukob_picking_mode",
      "gc_owms_usnb_picking_mode",
      "gc_owms_usot_picking_mode",
      "gc_owms_ussc_picking_mode",
      "zy_owms_au_picking_mode",
      "zy_owms_cz_picking_mode",
      "zy_owms_de_picking_mode",
      "zy_owms_ru_picking_mode",
      "zy_owms_uk_picking_mode",
      "zy_owms_usea_picking_mode",
      "zy_owms_uswe_picking_mode",
      "zy_owms_ussc_picking_mode"
    )
  }
  /*def pickingTables = {
    Array(      "gc_owms_au_advance_picking_detail",
      "gc_owms_cz_advance_picking_detail",
      "gc_owms_de_advance_picking_detail",
      "gc_owms_es_advance_picking_detail",
      "gc_owms_frvi_advance_picking_detail",
      "gc_owms_it_advance_picking_detail",
      "gc_owms_jp_advance_picking_detail",
      "gc_owms_uk_advance_picking_detail",
      "gc_owms_ukob_advance_picking_detail",
      "gc_owms_usea_advance_picking_detail",
      "gc_owms_uswe_advance_picking_detail",
      "gc_owms_usnb_advance_picking_detail",
      "gc_owms_usot_advance_picking_detail",
      "gc_owms_ussc_advance_picking_detail",
      "zy_owms_au_advance_picking_detail",
      "zy_owms_cz_advance_picking_detail",
      "zy_owms_de_advance_picking_detail",
      "zy_owms_ru_advance_picking_detail",
      "zy_owms_uk_advance_picking_detail",
      "zy_owms_usea_advance_picking_detail",
      "zy_owms_uswe_advance_picking_detail",
      "zy_owms_ussc_advance_picking_detail"
    )
  }*/
}