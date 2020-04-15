package com.zongteng.ztetl.entity.zy_wms

case class ReturnOrders (
                          ro_id:String,
                          receiving_code:String,
                          tracking_no:String,
                          ro_code:String,
                          customer_code:String,
                          order_code:String,
                          warehouse_id:String,
                          creater:String,
                          verifier:String,
                          expected_date:String,
                          ro_is_all:String,
                          ro_type:String,
                          ro_status:String,
                          ro_sync_status:String,
                          ro_process_type:String,
                          ro_create_type:String,
                          ro_desc:String,
                          ro_note:String,
                          ro_add_time:String,
                          ro_confirm_time:String,
                          ro_update_time:String,
                          shipping_method:String,
                          ro_weight:String,
                          sc_id:String,
                          logistics_type:String,
                          services1:String,
                          services2:String,
                          is_return:String,
                          ro_first_shelve_time:String,
                          ro_receiving_time:String,
                          ro_timestamp:String
                        )
