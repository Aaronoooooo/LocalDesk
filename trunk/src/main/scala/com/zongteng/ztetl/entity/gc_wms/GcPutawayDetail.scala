package com.zongteng.ztetl.entity.gc_wms

case class GcPutawayDetail(
                            pd_id: String,
                            receiving_code: String,
                            customer_code: String,
                            lc_code: String,
                            pd_inventory_type: String,
                            pd_type: String,
                            product_barcode: String,
                            pd_quantity: String,
                            warehouse_id: String,
                            pd_note: String,
                            pd_status: String,
                            putaway_user_id: String,
                            pd_add_time: String,
                            pd_update_time: String,
                            pd_putaway_time: String,
                            transit_receiving_code: String,
                            rd_id: String
                          )
