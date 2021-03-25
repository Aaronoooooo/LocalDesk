package com.mobikok.bigdata.spark.core.req.helper

import com.mobikok.bigdata.spark.core.req.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 热门品类累加器
  * 1. 继承AccumulatorV2，定义泛型【In, Out】
  *    IN : (品类，行为类型)
  *    OUT : Map[品类，HotCategory]
  * 2. 重写方法(6)
  */
class HotCategoryAccumulator extends AccumulatorV2[ (String, String),mutable.Map[String, HotCategory]]{

    val hotCategoryMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hotCategoryMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
        new HotCategoryAccumulator
    }

    override def reset(): Unit = {
        hotCategoryMap.clear()
    }

    override def add(v: (String, String)): Unit = {
        val cid = v._1
        val actionType = v._2

        val hotCategory = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))

        actionType match {
            case "click" => hotCategory.clickCount += 1
            case "order" => hotCategory.orderCount += 1
            case "pay"   => hotCategory.payCount += 1
            case _ =>
        }
        hotCategoryMap(cid) = hotCategory

    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
        other.value.foreach{
            case ( cid, hotCategory ) => {
                val hc = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))

                hc.clickCount += hotCategory.clickCount
                hc.orderCount += hotCategory.orderCount
                hc.payCount += hotCategory.payCount

                hotCategoryMap(cid) = hc
            }
        }
    }

    override def value: mutable.Map[String, HotCategory] = hotCategoryMap
}
