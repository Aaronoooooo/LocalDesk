package com.zongteng.ztetl.entity.gc_wms

case class FlowVolume (
                        fv_id:String,
                        product_id:String,
                        product_barcode:String,
                        customer_code:String,
                        warehouse_id:String,
                        warehouse_code:String,
                        wp_code:String,
                        fv_pending_quantity:String,
                        fv_quantity:String,
                        fv_adjustment_lock:String,
                        fv_add_time:String,
                        fv_update_time:String,
                        fv_processing_priority:String
                      )
