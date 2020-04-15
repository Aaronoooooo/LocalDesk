package com.zongteng.ztetl.common.zookeeper

import com.zongteng.ztetl.util.PropertyFile
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}

/**
  * zookeeper工具类
  */
object ZkConf {


  lazy private val zookeeperServer: String = {
    PropertyFile.getProperty("zookeeper_server")
  }

  def getZkClient(): ZkClient = {
    val sessionTimeout = 30000
    val connectionTimeout = 30000
    getZkClient(sessionTimeout, connectionTimeout)
  }

  def getZkClient(sessionTimeout: Int, connectionTimeout: Int): ZkClient = {
    ZkUtils.createZkClient(zookeeperServer, sessionTimeout, connectionTimeout)
  }

  def getZkClientAndConnection(sessionTimeout: Int, connectionTimeout: Int): (ZkClient, ZkConnection) = {
    ZkUtils.createZkClientAndConnection(zookeeperServer, sessionTimeout, connectionTimeout)
  }

  def getZkUtils(sessionTimeout: Int, connectionTimeout: Int): ZkUtils = {
    getZkUtils(sessionTimeout, connectionTimeout, true)
  }

  def getZkUtils(sessionTimeout: Int, connectionTimeout: Int, isSecure: Boolean): ZkUtils = {
    val zkClientAndZkConnection: (ZkClient, ZkConnection) = ZkUtils.createZkClientAndConnection(zookeeperServer, sessionTimeout, connectionTimeout)
    new ZkUtils(zkClientAndZkConnection._1, zkClientAndZkConnection._2, isSecure)
  }





}
