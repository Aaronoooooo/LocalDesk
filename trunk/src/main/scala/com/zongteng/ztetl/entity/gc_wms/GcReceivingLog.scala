package com.zongteng.ztetl.entity.gc_wms

case class GcReceivingLog (rl_id: String,
                           receiving_id: String,
                           receiving_code: String,
                           rl_type: String,
                           user_name: String,
                           user_code: String,
                           rl_status_from: String,
                           rl_status_to: String,
                           rl_note: String,
                           rl_add_time: String,
                           rl_ip: String,
                           rl_content: String
                          )