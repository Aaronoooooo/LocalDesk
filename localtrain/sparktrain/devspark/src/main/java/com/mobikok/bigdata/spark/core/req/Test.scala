package com.mobikok.bigdata.spark.core.req

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.bigdata.spark.core.req.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        // TODO 获取日志数据
        val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
        val actionRDD = rdd.map(
            line => {
                val datas = line.split("_")
                UserVisitAction(
                    datas(0),
                    datas(1).toLong,
                    datas(2),
                    datas(3).toLong,
                    datas(4),
                    datas(5),
                    datas(6).toLong,
                    datas(7).toLong,
                    datas(8),
                    datas(9),
                    datas(10),
                    datas(11),
                    datas(12).toLong
                )
            }
        )

        // TODO 更加会话ID对数据进行分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

        // TODO 对分组后的数据进行结构的改变
        val newRDD: RDD[(String, List[(Long, (Long, Int))])] = groupRDD.mapValues(
            iter => {
                // TODO 对session页面的访问顺序进行排序
                val actions: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
                // TODO 根据页面访问顺序获取页面的停留时间和次数（最后一个页面无法确定停留时间，暂时不需要考虑）
                // （起始页面ID, ( 页面跳转时间差，停留1次 )）
                actions.zip(actions.tail).map {
                    case (a1, a2) => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        val t1 = sdf.parse(a1.action_time).getTime
                        val t2 = sdf.parse(a2.action_time).getTime
                        (a1.page_id, (t2 - t1, 1))
                    }
                }
            }
        )
        // TODO 将转换结构后的数据按照起始页面进行统计
        val reduceRDD: RDD[(Long, (Long, Int))] = newRDD.map(_._2).flatMap(list => list).reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )
        // TODO 打印页面的平均停留时间。
        reduceRDD.collect.foreach{
            case ( pageid, (totaltime, count) ) => {
                println(
                    s"""| 页面【$pageid】总访问时长${totaltime}
                       | 页面【$pageid】总访问次数${count}
                       | 页面【$pageid】平均停留时长为 ：${totaltime/count}毫秒
                       | *******************************************************
                     """.stripMargin)
            }
        }

        sc.stop

    }
}
