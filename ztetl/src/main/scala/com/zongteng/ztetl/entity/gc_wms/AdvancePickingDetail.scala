package com.zongteng.ztetl.entity.gc_wms

case class AdvancePickingDetail (
    apd_id: String,
    warehouse_id: String,
    order_id: String,
    order_code: String,
    pd_status: String,
    pc_id: String,
    op_id: String,
    product_id: String,
    product_barcode: String,
    pd_quantity: String,
    ibo_id: String,
    ib_id: String,
    pv_id: String,
    pick_sort: String,
    pick_point: String,
    wa_code: String,
    lc_code: String,
    receiving_id: String,
    receiving_code: String,
    po_code: String,
    pd_add_time: String,
    is_flow_volume: String
)
