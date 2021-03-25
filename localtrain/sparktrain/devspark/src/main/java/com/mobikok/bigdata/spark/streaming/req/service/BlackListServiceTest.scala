package com.mobikok.bigdata.spark.streaming.req.service

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.bigdata.spark.streaming.req.bean.Ad_Click_Log
import com.mobikok.bigdata.spark.streaming.req.dao.BlackListDaoTest
import com.mobikok.dev.framework.core.TService
import com.mobikok.dev.framework.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

/**
  * @author Aaron
  * @date 2020/07/28
  */
class BlackListServiceTest extends TService {
  private val blackListDaoTest = new BlackListDaoTest

  /**
    * 数据分析
    *
    * @return
    */

  override def analysis() = {
    //读取kafka数据
    val ds: DStream[String] = blackListDaoTest.readKafka()

    val logDS: DStream[Ad_Click_Log] = ds.map(
      data => {
        val datas: Array[String] = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //1获取黑名单表信息,判读是否在黑名单中
    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        //1.1读取mysql数据库,获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(
          """
          select * from black_list
          """.stripMargin
        )
        val rs: ResultSet = pstat.executeQuery()
        // 1.2结果集中第一列黑名单ID存入可变集合List中
        val blackIds: ListBuffer[String] = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()
        //2 过滤已存在于黑名单中的用户,不做统计
        val filterRDD: RDD[Ad_Click_Log] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )
        //3 对正常数据进行黑名单统计(每天的点击量超过100次)
        //3.1计算点击次数
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )
        mapRDD.reduceByKey(_ + _)
      }
    )
    //4对统计结果中超过阈值的用户插入黑名单表
    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          datas => {
            //4.1更新用户点击表数据
            val connection1: Connection = JDBCUtil.getConnection()
            //4.2更新用户主键不重复时插入
            val pstat1: PreparedStatement = connection1.prepareStatement(
              """
                  insert into user_ad_count (dt,userid,adid,count)
                  values (?,?,?,?)
                  on duplicate key
                  update count = count + ?
              """.stripMargin)

            //5获取用户点击表数据超过阈值的用户id插入黑名单表
            val pstat: PreparedStatement = connection1.prepareStatement(
              """
                  insert into black_list ( userid )
                  select userid from user_ad_count
                  where dt = ? and userid = ? and adid = ? and count >= 100
                  on duplicate key
                  update userid = ?
                """.stripMargin)
            datas.foreach {
              case ((day, userid, adid), sum) => {
                //每一个采集周期(3s)中用户点击同一个广告的数量,按天统计需要有状态保存
                //updateStateByKey=>checkpoint=>HDFS=>产生大量的小文件
                //选择放入mysql或redis中

                pstat1.setString(1, day)
                pstat1.setString(2, userid)
                pstat1.setString(3, adid)
                pstat1.setLong(4, sum)
                pstat1.setLong(5, sum)
                pstat1.executeUpdate()

                pstat.setString(1, day)
                pstat.setString(2, userid)
                pstat.setString(3, adid)
                pstat.setString(4, userid)
                pstat.executeUpdate()
              }

            }
            pstat.close()
            connection1.close()
            pstat1.close()
            connection1.close()
          }
        )
      }
    )
  }

  def analysis1() = {
    //读取kafka数据
    val ds: DStream[String] = blackListDaoTest.readKafka()

    val logDS: DStream[Ad_Click_Log] = ds.map(
      data => {
        val datas: Array[String] = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //1获取黑名单表信息,判读是否在黑名单中
    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        //1.1读取mysql数据库,获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(
          """
          select * from black_list
          """.stripMargin
        )
        val rs: ResultSet = pstat.executeQuery()
        // 1.2结果集中第一列黑名单ID存入可变集合List中
        val blackIds: ListBuffer[String] = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()
        //2 过滤已存在于黑名单中的用户,不做统计
        val filterRDD: RDD[Ad_Click_Log] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )
        //3 对正常数据进行黑名单统计(每天的点击量超过100次)
        //3.1计算点击次数
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )
        mapRDD.reduceByKey(_ + _)
      }
    )
    //4对统计结果中超过阈值的用户插入黑名单表
    reduceDS.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, userid, adid), sum) => {
            //每一个采集周期(3s)中用户点击同一个广告的数量,按天统计需要有状态保存
            //updateStateByKey=>checkpoint=>HDFS=>产生大量的小文件
            //选择放入mysql或redis中
            //4.1更新用户点击表数据
            val connection1: Connection = JDBCUtil.getConnection()
            //4.2更新用户主键不重复时插入
            val pstat1: PreparedStatement = connection1.prepareStatement(
              """
                  insert into user_ad_count (dt,userid,adid,count)
                  values (?,?,?,?)
                  on duplicate key
                  update count = count + ?
              """.stripMargin)
            pstat1.setString(1,day)
            pstat1.setString(2,userid)
            pstat1.setString(3,adid)
            pstat1.setLong(4,sum)
            pstat1.setLong(5,sum)
            pstat1.executeUpdate()

            //5获取用户点击表数据超过阈值的用户id插入黑名单表
            val connection: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = connection.prepareStatement(
                """
                  insert into black_list ( userid )
                  select userid from user_ad_count
                  where dt = ? and userid = ? and adid = ? and count >= 100
                  on duplicate key
                  update userid = ?
                """.stripMargin)

            pstat.setString(1, day)
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setString(4, userid)
            pstat.executeUpdate()
            pstat.close()
            connection.close()
            pstat1.close()
            connection1.close()
          }
        }
      }
    )
  }
}
