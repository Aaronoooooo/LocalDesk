package com.zongteng.ztetl.entity.gc_owms.gc_owms_usnb

case class WellenRule (
    wellen_id: String,
    wms_wellen_id: String,
    wellen_name: String,
    warehouse_id: String,
    wellen_order_max: String,
    wellen_order_min: String,
    product_volume_max: String,
    product_weight_max: String,
    wellen_begin_time: String,
    wellen_end_time: String,
    customer_code: String,
    picker_id: String,
    wellen_time: String,
    wellen_space: String,
    wellen_sort: String,
    order_type: String,
    picking_mode: String,
    is_more_box: String,
    order_advance_pickup: String,
    wellen_area: String,
    wellen_status: String,
    is_sync_owms: String,
    wellen_code: String,
    picking_sort: String,
    advance_pickup_time: String,
    creater_id: String,
    create_time: String,
    creator_id: String,
    is_cross_warehouse: String
)
