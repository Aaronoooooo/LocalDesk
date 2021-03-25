package com.mobikok.bigdata.spark.core.req.service

import com.mobikok.bigdata.spark.core.req.bean
import com.mobikok.bigdata.spark.core.req.dao.PageflowDao
import com.mobikok.dev.framework.core.TService
import org.apache.spark.rdd.RDD

class PageflowService extends TService {

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

    /**
      * 数据分析
      *
      * @return
      */
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
}
