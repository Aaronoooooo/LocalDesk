package com.mobikok.bigdata.spark.core.req.service

import com.mobikok.bigdata.spark.core.req.bean
import com.mobikok.bigdata.spark.core.req.dao.{PageflowDaoTest}
import com.mobikok.dev.framework.core.TService
import org.apache.spark.rdd.RDD

class PageflowServiceTest extends TService {

  private val pageflowDao = new PageflowDaoTest

  /**
    * 数据分析
    *
    * @return
    */
  override def analysis() = {

    // TODO 对指定的页面流程进行页面单跳转换率的统计
    val flowIds = List(1, 2, 3, 4, 5, 6, 7)
    val okFlowIds: List[String] = flowIds.zip(flowIds.tail).map(t => t._1 + "-" + t._2)


    // TODO  获取原始用户行为日志数据
    val actionRDD: RDD[bean.UserVisitAction] =
      pageflowDao.getUserVisitAction("input/user_visit_action.txt")
    actionRDD.cache()

    // TODO 计算分母 过滤出单跳转换中最后一个页面
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        flowIds.init.contains(action.page_id.toInt)
      }
    )
    val pageToOneRDD: RDD[(Long, Int)] = filterRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()

    // TODO 计算分子
    // TODO 将数据根据用户Session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] =
    actionRDD.groupBy(_.session_id)

    val pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        // TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // TODO 将排序后的数据进行结构的转换,取出page_id
        //  action => pageid
        val pageids: List[Long] = actions.map(_.page_id)
        // TODO 将转换后的结果进行格式的转换
        // 1，2，3，4
        // 2，3，4
        // （1-2），（2-3），（3-4）
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }.filter{
          case (ids,one) => {
            okFlowIds.contains(ids)
          }
        }
      }
    )
    //去掉分组产生的key:
    // List1((14-10,1), (10-33,1), (33-27,1))
    // List2((24-5,1), (5-16,1))
    val pageidSumRDD: RDD[List[(String, Int)]] = pageFlowRDD.map(_._2)
    println("*******1*******")
    //pageidSumRDD.take(100).foreach(println)
    //压平RDD[List]:(14-10,1) (10-33,1) (33-27,1) (24-5,1), (5-16,1)
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list => list)
    println("*******2*******")
    //pageflowRDD1.take(1000).foreach(println)
    //相同key聚合
    val pageflowToSumRDD: RDD[(String, Int)] = pageflowRDD1.reduceByKey(_ + _)

    pageflowToSumRDD.foreach {
      case (pageflow, sum) => {
        val pageid: String = pageflow.split("-")(0)
        val value: Int = pageCountArray.toMap.getOrElse(pageid.toLong, 1)
        println("页面跳转 [" + pageflow + "]的转换率为: " + (sum.toDouble / value))
      }
    }
  }

  def analysis1() = {
    // TODO  获取原始用户行为日志数据
    val actionRDD: RDD[bean.UserVisitAction] =
      pageflowDao.getUserVisitAction("input/user_visit_action.txt")
    actionRDD.cache()

    // TODO 计算分母
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()

    // TODO 计算分子
    // TODO 将数据根据用户Session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] =
    actionRDD.groupBy(_.session_id)

    val pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        // TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // TODO 将排序后的数据进行结构的转换,取出page_id
        //  action => pageid
        val pageids: List[Long] = actions.map(_.page_id)
        // TODO 将转换后的结果进行格式的转换
        // 1，2，3，4
        // 2，3，4
        // （1-2），（2-3），（3-4）
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }
      }
    )
    //去掉分组产生的key:
    // List1((14-10,1), (10-33,1), (33-27,1))
    // List2((24-5,1), (5-16,1))
    val pageidSumRDD: RDD[List[(String, Int)]] = pageFlowRDD.map(_._2)
    println("*******1*******")
    //pageidSumRDD.take(100).foreach(println)
    //压平RDD[List]:(14-10,1) (10-33,1) (33-27,1) (24-5,1), (5-16,1)
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list => list)
    println("*******2*******")
    //pageflowRDD1.take(1000).foreach(println)
    //相同key聚合
    val pageflowToSumRDD: RDD[(String, Int)] = pageflowRDD1.reduceByKey(_ + _)

    pageflowToSumRDD.foreach {
      case (pageflow, sum) => {
        val pageid: String = pageflow.split("-")(0)
        val value: Int = pageCountArray.toMap.getOrElse(pageid.toLong, 1)
        println("页面跳转 [" + pageflow + "]的转换率为: " + (sum.toDouble / value))
      }
    }
  }
}
