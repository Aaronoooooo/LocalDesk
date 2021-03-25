package com.zongteng.ztetl.entity.zy_wms

case class ReceivingBoxDetail(
                               rbd_id: String,
                               rb_id: String,
                               product_id: String,
                               product_barcode: String,
                               quantity: String,
                               received_qty: String,
                               package_type: String,
                               product_package_type: String,
                               valid_date: String,
                               fba_product_code: String,
                               is_new_add: String,
                               rbx_timestamp: String,
                               add_new_box_quantity: String,
                               receiving_width: String,
                               receiving_height: String,
                               receiving_length: String,
                               receiving_weight: String,
                               received_weight: String,
                               received_length: String,
                               received_width: String,
                               received_height: String
                             )
