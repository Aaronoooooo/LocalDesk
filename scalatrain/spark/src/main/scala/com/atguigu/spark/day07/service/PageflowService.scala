package com.atguigu.spark.day07.service

import com.atguigu.spark.day07.bean.bean
import com.atguigu.spark.day07.dao.PageflowDao
import com.atguigu.summer.framework.core.TService
import org.apache.spark.rdd.RDD



class  PageflowService extends TService{

  private val pageflowDao = new PageflowDao

  /**
   * 数据分析
   *
   * @return
   */

  override def analysis() = {

    // TODO 对指定的页面流程进行页面单跳转换率的统计
    // 1，2，3，4，5，6，7
    // 1-2，2-3，3-4，4-5，5-6，6-7/
    val flowIds = List(1,2,3,4,5,6,7)
    val okFlowIds = flowIds.zip(flowIds.tail).map( t => (t._1 + "-" + t._2) )

    // TODO  获取原始用户行为日志数据
    val actionRDD: RDD[bean.UserVisitAction] =
      pageflowDao.getUserVisitAction("input/user_visit_action.txt")
    actionRDD.cache()

    // TODO 计算分母
    // 将数据进行过滤后再进行统计
    val filterRDD = actionRDD.filter(
      action => {
        flowIds.init.contains(action.page_id.toInt)
      }
    )
    val pageToOneRDD: RDD[(Long, Int)] = filterRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_+_)
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
        // TODO 将排序后的数据进行结构的转换
        //  action => pageid
        val pageids: List[Long] = actions.map(_.page_id)

        // TODO 将转换后的结果进行格式的转换
        // 1，2，3，4
        // 2，3，4
        // （1-2），（2-3），（3-4）
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)
        // （（1-2），1），（（2-3），1），（（3，4），1）
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }.filter{
          case ( ids, one ) => {
            okFlowIds.contains(ids)
          }
        }
      }
    )

    // TODO 将分组后的数据进行结构的转换
    val pageidSumRDD: RDD[List[(String, Int)]] = pageFlowRDD.map(_._2)
    // (1-2,1)
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list=>list)
    // (1-2,sum)
    val pageflowToSumRDD = pageflowRDD1.reduceByKey(_+_)

    // TODO 计算页面单跳转换率
    // 1-2/1
    pageflowToSumRDD.foreach{
      case ( pageflow, sum ) => {
        val pageid = pageflow.split("-")(0)
        val value = pageCountArray.toMap.getOrElse(pageid.toLong,1)
        println("页面跳转【"+pageflow+"】的转换率为: " + (sum.toDouble / value))
      }
    }
  }
  def analysis1() = {

    //获取数据
    val actionRDD: RDD[bean.UserVisitAction] = pageflowDao.getUserVisitAction("input/user_visit_action.txt")

    actionRDD.cache()
    //计算分母
    val mapRDD: RDD[(Long, Int)] = actionRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val reduceRDD: RDD[(Long, Int)] = mapRDD.reduceByKey(_+_)
    val pageCountArray: Array[(Long, Int)] = reduceRDD.collect()
    //计算分子
    //将数据根据用户Session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        //按时间排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        //将排序后的数据进行转换
        val pageIds: List[Long] = actions.map(_.page_id)
        //将转换后的结果进行格式的转换
        // 1，2，3，4
        // 2，3，4
        // （1-2），（2-3），（3-4）
        val zipIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        zipIds.map {
          case (zipId1, zipId2) => {
            ((zipId1 + "-" + zipId2), 1)
          }
        }
      }
    )
    //将分组后的数据进行转换
    val pageidSumRDD: RDD[List[(String, Int)]] = pageFlowRDD.map(_._2)
    val flatMapRDD: RDD[(String, Int)] = pageidSumRDD.flatMap(List=>List)
    val pageflowToSumRDD: RDD[(String, Int)] = flatMapRDD.reduceByKey(_+_)

    //计算页面单跳转换率
    pageflowToSumRDD.foreach{
      case ( pageflow, sum ) => {
        val pageid = pageflow.split("-")(0)
        val value = pageCountArray.toMap.getOrElse(pageid.toLong,1)
        println("页面跳转【"+pageflow+"】的转换率为: " + (sum.toDouble / value))
      }
    }

  }
}
