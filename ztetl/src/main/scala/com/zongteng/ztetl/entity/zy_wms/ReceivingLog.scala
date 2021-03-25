package com.zongteng.ztetl.entity.zy_wms

case class ReceivingLog(rl_id: String,
                        receiving_id: String,
                        receiving_code: String,
                        rl_type: String,
                        user_id: String,
                        customer_code: String,
                        rl_status_from: String,
                        rl_status_to: String,
                        rl_note: String,
                        rl_add_time: String,
                        rl_ip: String,
                        rl_content: String
                       )