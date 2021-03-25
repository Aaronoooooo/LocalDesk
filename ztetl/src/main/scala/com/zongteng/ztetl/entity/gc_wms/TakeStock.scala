package com.zongteng.ztetl.entity.gc_wms

case class TakeStock (
                       ts_id:String,
                       ts_code:String,
                       warehouse_id:String,
                       wp_code:String,
                       user_id:String,
                       ts_last_update_user_id:String,
                       ts_type:String,
                       ts_status:String,
                       ts_add_time:String,
                       ts_update_time:String,
                       ts_is_product:String,
                       ts_is_quantity:String,
                       ts_user_id:String
                     )
