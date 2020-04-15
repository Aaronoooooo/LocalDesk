package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

object Orders_stream_allow_mysql_table {

  // 允许那些表可以添加进来（mysql的表）
  val table = Array[String](
    // 入库单（6张表）
    "gc_wms_orders",
         "gc_wms_order_operation_time",
         "gc_wms_order_physical_relation",
         "zy_wms_orders",
         "zy_wms_order_operation_time",
         "zy_wms_order_physical_relation",

    // 交运（实时）
     "stream_gc_lms_au_container_details",
     "stream_gc_lms_cz_container_details",
     "stream_gc_lms_es_container_details",
     "stream_gc_lms_frvi_container_details",
     "stream_gc_lms_it_container_details",
     "stream_gc_lms_uk_container_details",
     "stream_gc_lms_uswe_container_details",
     "stream_gc_lms_usea_container_details",
     "stream_gc_lms_ussc_container_details"
  )



}
