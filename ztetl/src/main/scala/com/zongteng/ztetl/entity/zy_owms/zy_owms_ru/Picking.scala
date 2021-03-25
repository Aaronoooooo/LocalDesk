package com.zongteng.ztetl.entity.zy_owms.zy_owms_ru

case class Picking (
    picking_id: String,
    warehouse_id: String,
    picking_code: String,
    print_quantity: String,
    picker_id: String,
    creater_id: String,
    picking_order_cnt: String,
    picking_lc_cnt: String,
    picking_item_cnt: String,
    osot_code_str: String,
    picking_mode: String,
    picking_status: String,
    picking_sync_status: String,
    picking_sync_time: String,
    is_assign: String,
    picking_pack_check: String,
    picking_type: String,
    picking_add_time: String,
    picking_update_time: String,
    is_more_box: String,
    is_print: String,
    picking_sort: String,
    task_id: String,
    wellen_code: String
)
