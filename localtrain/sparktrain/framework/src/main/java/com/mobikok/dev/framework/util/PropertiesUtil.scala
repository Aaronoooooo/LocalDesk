package com.mobikok.dev.framework.util

import java.util.ResourceBundle


object PropertiesUtil {

  // 绑定配置文件
  // ResourceBundle专门用于读取配置文件,所以读取时,不需要增加扩展名(baseName:config.properties)
  // 国际化 = I18N => Properties
  val summer: ResourceBundle = ResourceBundle.getBundle("config")

  def getValue( key : String ): String = {
    summer.getString(key)
  }
}


