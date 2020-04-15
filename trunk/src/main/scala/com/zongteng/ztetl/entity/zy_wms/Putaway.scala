package com.zongteng.ztetl.entity.zy_wms

case class Putaway(
                    putaway_id: String,
                    putaway_code: String,
                    receiving_code: String,
                    warehouse_id: String,
                    putaway_note: String,
                    putaway_add_time: String,
                    putaway_update_time: String,
                    transit_receiving_code: String,
                    pa_timestamp: String
                  )
