package com.mobikok.bigdata.spark.core.req.helper

import com.mobikok.bigdata.spark.core.req.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @author Aaron
  * @date 2020/07/27
  */
class HotCategoryAccumulatorTest extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{

  val hotCategoryMap: mutable.Map[String, HotCategory] = mutable.Map[String,HotCategory]()

  override def isZero: Boolean = {
    hotCategoryMap.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
    new HotCategoryAccumulatorTest
  }

  override def reset(): Unit = {
    hotCategoryMap.clear()
  }

  override def add(v: (String, String)): Unit = {
    val cid: String = v._1
    val actionType: String = v._2
    val hotCategory: HotCategory = hotCategoryMap.getOrElse(cid,HotCategory(cid,0,0,0))
    actionType match {
      case "click" => hotCategory.clickCount += 1
      case "order" => hotCategory.orderCount += 1
      case "pay" => hotCategory.payCount += 1
      case _ =>
    }
    hotCategoryMap(cid) = hotCategory

  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
other.value.foreach{
  case (cid,hotCategory) => {
    val hc: HotCategory = hotCategoryMap.getOrElse(cid,HotCategory(cid, 0, 0, 0))
    hc.clickCount += hotCategory.clickCount
    hc.orderCount += hotCategory.orderCount
    hc.payCount += hotCategory.payCount
    hotCategoryMap(cid) = hc

  }
}
  }

  override def value: mutable.Map[String, HotCategory] = {
    hotCategoryMap
  }
}
