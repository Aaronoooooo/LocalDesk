package com.mobikok.bigdata.spark.core.req.service

import com.mobikok.bigdata.spark.core.req.bean
import com.mobikok.bigdata.spark.core.req.bean.HotCategory
import com.mobikok.bigdata.spark.core.req.dao.{HotCategorySessionAnalysisTop10DaoTest}
import com.mobikok.bigdata.spark.core.req.helper.HotCategoryAccumulatorTest
import com.mobikok.dev.framework.core.TService
import com.mobikok.dev.framework.util.EnvUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @author Aaron
  * @date 2020/07/26
  */
class HotCategorySessionAnalysisTop10ServiceTest extends TService {
  private val hotCategorySessionAnalysisTop10DaoTest = new HotCategorySessionAnalysisTop10DaoTest


  /**
    * 数据分析
    *
    * @return
    */

  override def analysis(data: Any) = {
    val top10: List[HotCategory] = data.asInstanceOf[List[HotCategory]]
    val top10Ids: List[String] = top10.map(_.categoryId)

    //广播变量传递top10Ids
    val bcList: Broadcast[List[String]] = EnvUtil.getEnv().broadcast(top10Ids)

    val actionRDD: RDD[bean.UserVisitAction] = hotCategorySessionAnalysisTop10DaoTest.getUserVisitAction("input/user_visit_action.txt")


    //过滤数据
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          bcList.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )
    val rdd: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    //转换结构
    //(品类_session,sum) => (品类,(session,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }).take(10)
      }
    )
    resultRDD.collect()
  }

  def analysis2(data: Any) = {
    val top10: List[HotCategory] = data.asInstanceOf[List[HotCategory]]
    val top10Ids: List[String] = top10.map(_.categoryId)

    val actionRDD: RDD[bean.UserVisitAction] = hotCategorySessionAnalysisTop10DaoTest.getUserVisitAction("input/user_visit_action.txt")

    //过滤数据
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          top10Ids.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )
    val rdd: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    //转换结构
    //(品类_session,sum) => (品类,(session,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }).take(10)
      }
    )
    resultRDD.collect()
  }

  def analysis1(data: Any) = {
    val top10: List[HotCategory] = data.asInstanceOf[List[HotCategory]]

    val actionRDD: RDD[bean.UserVisitAction] = hotCategorySessionAnalysisTop10DaoTest.getUserVisitAction("input/user_visit_action.txt")

    //过滤数据
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          var flg = false
          top10.foreach(
            hc => {
              if (hc.categoryId.toLong == action.click_category_id) {
                flg = true
              }
            }
          )
          flg
        } else {
          false
        }
      }
    )
    val rdd: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    //转换结构
    //(品类_session,sum) => (品类,(session,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }).take(10)
      }
    )
    resultRDD.collect()
  }
}
