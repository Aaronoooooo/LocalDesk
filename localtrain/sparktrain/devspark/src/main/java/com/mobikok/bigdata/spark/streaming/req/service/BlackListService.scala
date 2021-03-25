package com.mobikok.bigdata.spark.streaming.req.service

import java.sql.{Connection, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.bigdata.spark.streaming.req.bean.Ad_Click_Log
import com.mobikok.bigdata.spark.streaming.req.dao.BlackListDao
import com.mobikok.dev.framework.core.TService
import com.mobikok.dev.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

class BlackListService extends TService {

  private val blackListDao = new BlackListDao

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {

    // TODO 读取Kafka的数据
    val ds: DStream[String] = blackListDao.readKafka()
    ds.print()
    // TODO 将数据转换为样例类来使用
    val logDS = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中
    // Code (1)
    // Transform
    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        // Code ： 周期性执行
        // TODO 读取Mysql数据库，获取黑名单信息
        // JDBC
        val connection: Connection = JDBCUtil.getConnection()
        val pstat = connection.prepareStatement(
          """
                    select userid from black_list
                    """.stripMargin)

        val rs: ResultSet = pstat.executeQuery()
        // 黑名单的ID集合
        val blackIds = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close
        pstat.close
        connection.close()

        // TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )



        // TODO 将正常的数据进行点击量的统计
        //     (key, 1) =>（key, sum）
        // key = day - userid - adid
        // ("2012-12-20" - userid - adid, 1) => (day - userid - adid, sum)
        // ts => day
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )

        mapRDD.reduceByKey(_ + _)
      }
    )

    // TODO 将统计的结果中超过阈值的用户信息拉人到黑名单中。
    // TODO 数据库的连接对象是无法序列化的。
    reduceDS.foreachRDD(
      rdd => {
        // TODO RDD可以以分区为单位进行数据的遍历
        rdd.foreachPartition(
          datas => {
            val conn: Connection = JDBCUtil.getConnection()
            val pstat = conn.prepareStatement(
              """
                |insert into user_ad_count ( dt, userid, adid, count )
                |values ( ?, ?, ?, ? )
                |on duplicate key
                |update count = count + ?
                            """.stripMargin)

            val pstat1 = conn.prepareStatement(
              """
                | select
                |     userid
                | from user_ad_count
                | where dt = ? and userid = ? and adid = ? and count >= 100
                            """.stripMargin)

            val pstat2 = conn.prepareStatement(
              """
                | insert into black_list(userid)
                | values (?)
                | on duplicate key
                | update userid = ?
                            """.stripMargin)

            datas.foreach {
              case ((day, userid, adid), sum) => {
                pstat.setString(1, day)
                pstat.setString(2, userid)
                pstat.setString(3, adid)
                pstat.setLong(4, sum)
                pstat.setLong(5, sum)
                pstat.executeUpdate()

                // TODO 获取最新的用户统计数据
                pstat1.setString(1, day)
                pstat1.setString(2, userid)
                pstat1.setString(3, adid)
                val rs1 = pstat1.executeQuery()
                if (rs1.next()) {
                  // TODO 判断是否超过阈值
                  // TODO 如果超过阈值，拉入到黑名单
                  //  20 - 1 - 20  100
                  //  20 - 1 - 21  100

                  pstat2.setString(1, userid)
                  pstat2.setString(2, userid)
                  pstat2.executeUpdate()

                }
              }
            }
            pstat2.close()
            pstat.close()
            pstat1.close()
            conn.close()
          }
        )
      }
    )


    ds.print()
  }

  /**
   * 数据分析
   *
   * @return
   */
  def analysis2() = {

    // TODO 读取Kafka的数据
    val ds: DStream[String] = blackListDao.readKafka()
    // TODO 将数据转换为样例类来使用
    val logDS = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中
    // Code (1)
    // Transform
    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        // Code ： 周期性执行
        // TODO 读取Mysql数据库，获取黑名单信息
        // JDBC
        val connection: Connection = JDBCUtil.getConnection()
        val pstat = connection.prepareStatement(
          """
                    select userid from black_list
                    """.stripMargin)

        val rs: ResultSet = pstat.executeQuery()
        // 黑名单的ID集合
        val blackIds = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close
        pstat.close
        connection.close()

        // TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )

        // TODO 将正常的数据进行点击量的统计
        //     (key, 1) =>（key, sum）
        // key = day - userid - adid
        // ("2012-12-20" - userid - adid, 1) => (day - userid - adid, sum)
        // ts => day
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )

        mapRDD.reduceByKey(_ + _)
      }
    )

    // TODO 将统计的结果中超过阈值的用户信息拉人到黑名单中。
    // TODO 数据库的连接对象是无法序列化的。
    reduceDS.foreachRDD(
      rdd => {

        val conn: Connection = JDBCUtil.getConnection()
        val pstat = conn.prepareStatement(
          """
            |insert into user_ad_count ( dt, userid, adid, count )
            |values ( ?, ?, ?, ? )
            |on duplicate key
            |update count = count + ?
                    """.stripMargin)

        val pstat1 = conn.prepareStatement(
          """
            | select
            |     userid
            | from user_ad_count
            | where dt = ? and userid = ? and adid = ? and count >= 100
                    """.stripMargin)

        val pstat2 = conn.prepareStatement(
          """
            | insert into balck_list(userid)
            | values (?)
            | on duplicate key
            | update userid = ?
                    """.stripMargin)

        rdd.foreach {
          case ((day, userid, adid), sum) => {
            pstat.setString(1, day)
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setLong(4, sum)
            pstat.setLong(5, sum)
            pstat.executeUpdate()

            // TODO 获取最新的用户统计数据
            pstat1.setString(1, day)
            pstat1.setString(2, userid)
            pstat1.setString(3, adid)
            val rs1 = pstat.executeQuery()
            if (rs1.next()) {
              // TODO 判断是否超过阈值
              // TODO 如果超过阈值，拉入到黑名单
              //  20 - 1 - 20  100
              //  20 - 1 - 21  100

              pstat2.setString(1, userid)
              pstat2.setString(2, userid)
              pstat2.executeUpdate()
              pstat2.close()
            }
            pstat.close()
            pstat1.close()
            conn.close()
          }
        }
      }
    )


    ds.print()
  }

  /**
   * 数据分析
   *
   * @return
   */
  def analysis1() = {

    // TODO 读取Kafka的数据
    val ds: DStream[String] = blackListDao.readKafka()
    // TODO 将数据转换为样例类来使用
    val logDS = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中
    // Code (1)
    // Transform
    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        // Code ： 周期性执行
        // TODO 读取Mysql数据库，获取黑名单信息
        // JDBC
        val connection: Connection = JDBCUtil.getConnection()
        val pstat = connection.prepareStatement(
          """
                    select userid from black_list
                    """.stripMargin)

        val rs: ResultSet = pstat.executeQuery()
        // 黑名单的ID集合
        val blackIds = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close
        pstat.close
        connection.close()

        // TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )

        // TODO 将正常的数据进行点击量的统计
        //     (key, 1) =>（key, sum）
        // key = day - userid - adid
        // ("2012-12-20" - userid - adid, 1) => (day - userid - adid, sum)
        // ts => day
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )

        mapRDD.reduceByKey(_ + _)
      }
    )

    // TODO 将统计的结果中超过阈值的用户信息拉人到黑名单中。
    reduceDS.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, userid, adid), sum) => {
            // Data : 每一个采集周期中用户点击同一个广告的数量
            // 有状态保存
            // updateStateByKey=>checkpoint=>HDFS=>产生大量的小文件
            // 统计结果应该是放在mysql（redis,过期数据）中
            // TODO 更新（新增）用户的点击数量
            val conn: Connection = JDBCUtil.getConnection()
            val pstat = conn.prepareStatement(
              """
                |insert into user_ad_count ( dt, userid, adid, count )
                |values ( ?, ?, ?, ? )
                |on duplicate key
                |update count = count + ?
                            """.stripMargin)

            pstat.setString(1, day)
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setLong(4, sum)
            pstat.setLong(5, sum)
            pstat.executeUpdate()

            // TODO 获取最新的用户统计数据
            val pstat1 = conn.prepareStatement(
              """
                | select
                |     userid
                | from user_ad_count
                | where dt = ? and userid = ? and adid = ? and count >= 100
                            """.stripMargin)
            pstat1.setString(1, day)
            pstat1.setString(2, userid)
            pstat1.setString(3, adid)
            val rs1 = pstat.executeQuery()
            if (rs1.next()) {
              // TODO 判断是否超过阈值
              // TODO 如果超过阈值，拉入到黑名单
              //  20 - 1 - 20  100
              //  20 - 1 - 21  100
              val pstat2 = conn.prepareStatement(
                """
                  | insert into balck_list(userid)
                  | values (?)
                  | on duplicate key
                  | update userid = ?
                                """.stripMargin)
              pstat2.setString(1, userid)
              pstat2.setString(2, userid)
              pstat2.executeUpdate()
              pstat2.close()
            }
            pstat1.close()
            conn.close()





            // insert black_list select xxxx where count > 100 update
            // insert ....select...
            //                        val pstat1 = conn.prepareStatement(
            //                            """
            //                              |insert into black_list ( userid )
            //                              |select userid from user_ad_count
            //                              |where dt = ? and userid = ? and adid = ? and count >= 100
            //                              |on duplicate key
            //                              |update userid = ?
            //                            """.stripMargin)
            //
            //                        pstat1.setString(1, day)
            //                        pstat1.setString(2, userid)
            //                        pstat1.setString(3, adid)
            //                        pstat1.setString(4, userid)
            //                        pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()
          }
        }
      }
    )


    ds.print()
  }
}
