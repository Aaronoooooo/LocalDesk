package com.zongteng.ztetl.entity.gc_owms.gc_owms_cz

case class NewWellenRule (
    wellen_id: String,
    wellen_code: String,
    warehouse_id: String,
    is_cross_warehouse: String,
    wellen_name: String,
    wellen_order_max: String,
    wellen_order_min: String,
    wellen_sort: String,
    ct_id: String,
    sorting_mode: String,
    customer_contain: String,
    order_type: String,
    is_more_box: String,
    advance_pickup_time: String,
    picking_mode: String,
    order_pick_type: String,
    is_spec: String,
    wellen_area: String,
    wellen_roadway: String,
    order_volume_max: String,
    order_volume_min: String,
    wellen_status: String,
    pick_volume_type: String,
    creater_id: String,
    create_time: String,
    update_id: String,
    update_time: String
)
