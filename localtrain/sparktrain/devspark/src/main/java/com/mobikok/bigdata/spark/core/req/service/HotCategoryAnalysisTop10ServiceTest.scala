package com.mobikok.bigdata.spark.core.req.service

import com.mobikok.bigdata.spark.core.req.bean.HotCategory
import com.mobikok.bigdata.spark.core.req.dao.HotCategoryAnalysisTop10DaoTest
import com.mobikok.bigdata.spark.core.req.helper.HotCategoryAccumulatorTest
import com.mobikok.dev.framework.core.TService
import com.mobikok.dev.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @author Aaron
  * @date 2020/07/26
  */
class HotCategoryAnalysisTop10ServiceTest extends TService {
  private val hotCategoryAnalysisTop10DaoTest = new HotCategoryAnalysisTop10DaoTest


  /**
    * 数据分析
    *
    * @return
    */
  override def analysis() = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DaoTest.readFile("input/user_visit_action.txt")
      val acc = new HotCategoryAccumulatorTest
    EnvUtil.getEnv().register(acc, "hotCategory")

    // TODO 将数据循环，向累加器中放
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if ( datas(6) != "-1" ) {
          // 点击的场合
          acc.add(( datas(6), "click" ))
        } else if ( datas(8) != "null" ) {
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add((id, "order"))
            }
          )
        } else if ( datas(10) != "null" ) {
          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add((id, "pay"))
            }
          )
        } else {
          Nil
        }
      }
    )

    // TODO 获取累加器的值
    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)

    categories.toList.sortWith(
      (leftHC, rightHC) => {
        if ( leftHC.clickCount > rightHC.clickCount ) {
          true
        } else if ( leftHC.clickCount == rightHC.clickCount ) {
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

  }

  def analysis5() = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DaoTest.readFile("input/user_visit_action.txt")
    val countRDD: RDD[(String, HotCategory)] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), HotCategory(datas(6), 1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, HotCategory(id, 0, 0, 1)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, HotCategory(id, 0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val reduceRDD: RDD[(String, HotCategory)] = countRDD.reduceByKey(
      (c1, c2) => {
        c1.clickCount = c1.clickCount + c2.clickCount
        c1.orderCount = c1.orderCount + c2.orderCount
        c1.payCount = c1.payCount + c2.payCount
        c1
      }
    )
    reduceRDD.collect().sortWith(
      (left,right) => {
        val leftHC: HotCategory = left._2
        val rightHC: HotCategory = right._2
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount){
          if(leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount> rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }

      }
    ).take(10)
  }

  def analysis4() = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DaoTest.readFile("input/user_visit_action.txt")
    val countRDD: RDD[(String, HotCategory)] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), HotCategory(datas(6), 1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, HotCategory(id, 0, 0, 1)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, HotCategory(id, 0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val reduceRDD: RDD[(String, HotCategory)] = countRDD.reduceByKey(
      (c1, c2) => {
        c1.clickCount = c1.clickCount + c2.clickCount
        c1.orderCount = c1.orderCount + c2.orderCount
        c1.payCount = c1.payCount + c2.payCount
        c1
      }
    )
    reduceRDD.collect().sortWith(
      (left,right) => {
        val leftHC: HotCategory = left._2
        val rightHC: HotCategory = right._2
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount){
          if(leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount> rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }

      }
    ).take(10)
  }

  def analysis3() = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DaoTest.readFile("input/user_visit_action.txt")
    val countRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap {
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    }
    val reduceRDD: RDD[(String, (Int, Int, Int))] = countRDD.reduceByKey {
      (v1, v2) => {
        (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
      }
    }
    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false)
    sortRDD.take(10).foreach(println)
    println(""""""""""""""")
  }

  def analysis2() = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DaoTest.readFile("input/user_visit_action.txt")
    //计算结果缓存
    actionRDD.cache()
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).filter(_._1 != "-1")
    //println(clickRDD.collect().mkString("||"))
    //相同的key做count
    val categoryIdToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)
    //println(categoryIdToClickCountRDD.collect().mkString("|||"))

    val orderRDD: RDD[String] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8)
      }
    ).filter(_ != "null")
    //println(orderRDD.collect().mkString("||"))
    val orderToOneRDD: RDD[(String, Int)] = orderRDD.flatMap(
      id => {
        val ids: mutable.ArrayOps[String] = id.split(",")
        ids.map((_, 1))
      }
    )
    //println(orderToOneRDD.collect().mkString("|||"))
    val categroyIdToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_ + _)
    //println(categroyIdToOrderCountRDD.collect().mkString("||||"))

    val payRDD: RDD[String] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10)
      }
    ).filter(_ != "null")
    val payToOneRDD: RDD[(String, Int)] = payRDD.flatMap(
      id => {
        val ids: mutable.ArrayOps[String] = id.split(",")
        ids.map((_, 1))
      }
    )
    val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_ + _)

    // 转换结构
    val newCategoryIdToClickCountRDD: RDD[(String, (Int, Int, Int))] = categoryIdToClickCountRDD.map {
      case (id, clickCount) => {
        (id, (clickCount, 0, 0))
      }
    }
    val newCategoryIdToOrderCountRDD: RDD[(String, (Int, Int, Int))] = categroyIdToOrderCountRDD.map {
      case (id, orderCount) => {
        (id, (0, orderCount, 0))
      }
    }
    var newCategoryIdToPayCountRDD = categoryIdToPayCountRDD.map {
      case (id, payCount) => {
        (id, (0, 0, payCount))
      }
    }

    val countRDD: RDD[(String, (Int, Int, Int))] = newCategoryIdToClickCountRDD.union(newCategoryIdToOrderCountRDD).union(newCategoryIdToPayCountRDD)
    val reduceRDD: RDD[(String, (Int, Int, Int))] = countRDD.reduceByKey {
      (v1, v2) => {
        (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
      }
    }
    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false)
    sortRDD.take(10).foreach(println)
    println(""""""""""""""")
  }

  /**
    * 数据分析
    *
    * @return
    */
  def analysis1() = {
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DaoTest.readFile("input/user_visit_action.txt")
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).filter(_._1 != "-1")
    //println(clickRDD.collect().mkString("||"))
    //相同的key做count
    val categoryIdToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)
    //println(categoryIdToClickCountRDD.collect().mkString("|||"))

    val orderRDD: RDD[String] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8)
      }
    ).filter(_ != "null")
    //println(orderRDD.collect().mkString("||"))
    val orderToOneRDD: RDD[(String, Int)] = orderRDD.flatMap(
      id => {
        val ids: mutable.ArrayOps[String] = id.split(",")
        ids.map((_, 1))
      }
    )
    //println(orderToOneRDD.collect().mkString("|||"))
    val categroyIdToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_ + _)
    //println(categroyIdToOrderCountRDD.collect().mkString("||||"))

    val payRDD: RDD[String] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10)
      }
    ).filter(_ != "null")
    val payToOneRDD: RDD[(String, Int)] = payRDD.flatMap(
      id => {
        val ids: mutable.ArrayOps[String] = id.split(",")
        ids.map((_, 1))
      }
    )
    val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_ + _)

    // 转换结构
    val joinRDD: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join(categroyIdToOrderCountRDD)
    joinRDD.foreach(println)
    val joinRDD1: RDD[(String, ((Int, Int), Int))] = joinRDD.join(categoryIdToPayCountRDD)
    joinRDD1.foreach(println)
    val mapRDD: RDD[(String, (Int, Int, Int))] = joinRDD1.mapValues {
      case ((clickCount, orderCount), payCount) => {
        (clickCount, orderCount, payCount)
      }
    }
    val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, false)
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)
    println(""""""""""""""")
    result.foreach(println)
  }
}
