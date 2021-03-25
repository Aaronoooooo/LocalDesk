package com.zongteng.ztetl.etl.stream.gc_wms_orders.fact

import org.apache.commons.lang3.StringUtils

object StreamTypeParaUtil {


  def nvlDateTime(dateTime: String) :String = {

    if (StringUtils.isBlank(dateTime) || (dateTime.substring(0, 4).toInt == 0)) {
      null
    } else {
      dateTime
    }

  }


}
