package com.zongteng.ztetl.entity.zy_owms.zy_owms_uswe

case class PickingDetail (
    pd_id: String,
    picking_id: String,
    picking_code: String,
    order_id: String,
    order_code: String,
    pd_status: String,
    op_id: String,
    product_id: String,
    product_barcode: String,
    pd_quantity: String,
    scan_quantity: String,
    ibo_id: String,
    ib_id: String,
    pv_id: String,
    pick_point: String,
    pick_sort: String,
    lc_code: String,
    receiving_id: String,
    receiving_code: String,
    po_code: String,
    pd_fifo_time: String,
    pd_add_time: String,
    pd_update_time: String
)
