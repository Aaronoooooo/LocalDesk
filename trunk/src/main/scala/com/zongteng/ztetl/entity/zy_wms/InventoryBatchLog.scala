package com.zongteng.ztetl.entity.zy_wms

case class InventoryBatchLog (
                               ibl_id :String,
                               ref_no :String,
                               lc_code :String,
                               product_id :String,
                               product_barcode :String,
                               supplier_id :String,
                               warehouse_id :String,
                               ib_id :String,
                               receiving_code :String,
                               po_code :String,
                               box_code :String,
                               reference_no :String,
                               application_code :String,
                               ibl_note :String,
                               ibl_quantity_before :String,
                               ibl_quantity_after :String,
                               user_id :String,
                               ibl_ip :String,
                               ibl_add_time :String
                             )
