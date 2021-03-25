package com.zongteng.ztetl.entity.gc_wms

case class OrderLog(
                     ol_id: String,
                     order_id: String,
                     order_code: String,
                     ol_type: String,
                     order_status_from: String,
                     order_status_to: String,
                     ol_add_time: String,
                     user_id: String,
                     ol_ip: String,
                     ol_comments: String
                   )
