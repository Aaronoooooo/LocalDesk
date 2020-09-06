package com.flinksqlscala

import org.apache.http.HttpHost


/**
 * ES工具类
 */
object EsUtil extends  Serializable {

  /**
   * 获取ES http连接
   */
  def getEsHttpConnect:java.util.ArrayList[HttpHost]={
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("CentOS7",9200,"http"))
    httpHosts
  }

}
