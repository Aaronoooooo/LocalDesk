package com.atguigu.scala.day09

/**
 * * @create 2020-05-29 20:29
 */
object TestData {

  def main(args: Array[String]): Unit = {
    val dataList = List(
      ("zhangsan", "河北", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "鞋"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "鞋"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "鞋"),
      ("zhangsan", "河北", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "帽子"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "电脑"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "电脑"),
      ("zhangsan", "河南", "电脑"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子")
    )
    //1.将数据进行清洗
    //("wangwu", "河北", "帽子")-->("河北-帽子")
    val list: List[String] = dataList.map(
      t => {
        (t._2 + "-" + t._3)
      }
    )
    //2.将数据按照省份和商品分组并求出数量
    //("河北-帽子")-->("河北-帽子",count)
    val dataGroupMap: Map[String, List[String]] = list.groupBy(data => data)
    val dataCountMap: Map[String, Int] = dataGroupMap.mapValues(_.size)
    //3.将数据进行结构的转换
    //("河北-帽子",count)-->("河北",("帽子",count))
    val preToList: List[(String, (String, Int))] = dataCountMap.toList.map(
      kv => {
        val k = kv._1
        val count = kv._2
        //将k切分
        val ks: Array[String] = k.split("-")
        (ks(0), (ks(1), count))
      }
    )
    //4.再按照省份分组
    //List("河北",List(("帽子",count)),("衣服",count))
    val groupMap: Map[String, List[(String, (String, Int))]] = preToList.groupBy(_._1)
    //5.排序，降序，打印
    val result: Map[String, List[(String, Int)]] = groupMap.mapValues(list => {
      val tuples: List[(String, Int)] = list.map(_._2)
      tuples.sortWith(
        (Left, Right) => {
          Left._2 > Right._2
        })
    })
    println(result)

    //    //1.进行数据的清洗
    //    //("wangwu", "河北", "帽子") => ("河北", "帽子") => ("河北-帽子")
    //    val list: List[String] = dataList.map(
    //      t => {
    //        (t._2 + "-" + t._3)
    //      }
    //    )
    //    //2.根据省份和商品同时进行分组
    //    val dataToMap: Map[String, List[String]] = list.groupBy(data=>data)
    //    //3.分组后求出商品数量
    //    //("河北-帽子") => ("河北-帽子", count)
    //    val dataToCountMap: Map[String, Int] = dataToMap.mapValues(_.size)
    //    //4.将分组聚合后的数据进行结构的转换
    //    //如果改变数据结构时，可能会导致key重复，那么不要使用map结构
    //    //("河北-帽子", count) => ( 河北, (帽子，count) )
    //    val prvToItemAndCountList: List[(String, (String, Int))] = dataToCountMap.toList.map(
    //      kv => {
    //        val k = kv._1
    //        val count = kv._2
    //        //将数据切分
    //        val ks: Array[String] = k.split("-")
    //        (ks(0), (ks(1), count))
    //      })
    //
    //    //5.将转换好的数据分组
    //    // ( 河北, List[(帽子，count),(鞋，count),(衣服，count) ])
    //    val groupMap: Map[String, List[(String, (String, Int))]] = prvToItemAndCountList.groupBy(_._1)
    //    //6.排序，降序
    //    val result: Map[String, List[(String, Int)]] = groupMap.mapValues(list => {
    //      val tuples: List[(String, Int)] = list.map(_._2)
    //      tuples.sortWith(
    //        (Left, Right) => {
    //          Left._2 > Right._2
    //        })
    //    })
    //    println(result)

    //    //1.进行数据的清洗
    //    val list: List[String] = dataList.map(
    //      t => {
    //        (t._2 + "-" + t._3)
    //      }
    //    )
    //
    //    //2.根据省份和商品同时进行分组
    //    val dataToListMap: Map[String, List[String]] = list.groupBy(data => data)
    //    //3.分组后求出商品数量
    //    val dataToCountMap: Map[String, Int] = dataToListMap.mapValues(_.size)
    //    //4.将分组聚合后的数据进行结构的转换
    //    // 如果改变数据结构时，可能会导致key重复，那么不要使用map结构
    //    // ("河北-帽子", count) => ( 河北, (帽子，count) )
    //    // ("河北-衣服", count) => ( 河北, (衣服，count) )
    //    val prvToItemAndCountList: List[(String, (String, Int))] = dataToCountMap.toList.map(
    //      kv => {
    //        val k = kv._1
    //        val count = kv._2
    //        //将k拆分
    //        val ks: Array[String] = k.split("-")
    //        (ks(0), (ks(1), count))
    //      }
    //    )
    //    //5.将得到的数据再分组
    //     val groupMap: Map[String, List[(String, (String, Int))]] = prvToItemAndCountList.groupBy(_._1)
    //    //6排序，降序
    //    val result: Map[String, List[(String, Int)]] = groupMap.mapValues(list => {
    //      val tuples: List[(String, Int)] = list.map(_._2)
    //      tuples.sortWith(
    //        (Left, Right) => {
    //          Left._2 > Right._2
    //        })
    //    })
    //    println(result)


    //    //  数据会存在多余的内容，应该将数据进行清洗
    //    //("wangwu", "河北", "帽子") => ("河北", "帽子") => ("河北-帽子")
    //    val list: List[String] = dataList.map(
    //      t => (t._2 + "-" + t._3)
    //    )
    //
    //    //应该在统计数据时，根据省份和商品同时进行分组
    //    val groupMap: Map[String, List[String]] = list.groupBy(data => data)
    //    val dataToCountMap: Map[String, Int] = groupMap.mapValues(_.size)
    //    //将分组聚合后的数据进行结构的转换
    //    // 如果改变数据结构时，可能会导致key重复，那么不要使用map结构
    //    val countList: List[(String, (String, Int))] = dataToCountMap.toList.map(
    //      kv => {
    //        val k = kv._1
    //        val count = kv._2
    //        // 将key进行拆分
    //        val ks: Array[String] = k.split("-")
    //        (ks(0), (ks(1), count))
    //
    //      }
    //    )
    //
    //    // 将分组聚合后的数据根据省份进行分组
    //    // ( 河北, List[(帽子，count),(鞋，count),(衣服，count) ])
    //
    //    val stringToMap: Map[String, List[(String, (String, Int))]] = countList.groupBy(_._1)
    //    //将分组后的数据进行排序：降序
    //    val result: Map[String, Unit] = stringToMap.mapValues(list => {
    //      val list1: List[(String, Int)] = list.map(_._2)
    //      list1.sortWith(
    //        (Left, Right) => {
    //          Left._2 > Right._2
    //        }
    //      )
    //    }
    //    )
    //    println(result)


  }

}
