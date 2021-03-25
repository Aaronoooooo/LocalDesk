package com.zongteng.ztetl.entity.zy_wms

case class OrderOperationTime(
                               oot_id: String,
                               order_id: String,
                               order_code: String,
                               cutoff_finish_time: String,
                               cutoff_time: String,
                               submit_time: String,
                               abnormal_time: String,
                               process_time: String,
                               pack_time: String,
                               ship_time: String,
                               import_time: String,
                               delivered_time: String,
                               sync_time: String,
                               update_time: String,
                               abnormal_user_id: String,
                               process_user_id: String,
                               pack_user_id: String,
                               import_user_id: String,
                               ship_user_id: String,
                               delivered_user_id: String,
                               cutoff_user_id: String,
                               cutoff_finish_user_id: String,
                               oot_timestamp: String,
                               ascan_time: String
                             )
