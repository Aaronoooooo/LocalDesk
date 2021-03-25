package com.atguigu.scala.day08

/**
 * @author zxjgreat
 * @create 2020-05-27 21:53
 */

/*
数据 ：
     List(
         ("hello", 4),
         ("hello spark", 3),
         ("hello spark scala", 2),
         ("hello spark scala hive", 1)
     )

要求 ： 1. 将上面的数据进行WordCount后排序取前三名！
        2. 使用2种不同的方式。
 */
object WordCount1 {
  def main(args: Array[String]): Unit = {

    val dataList = List(
      ("hello", 4),
      ("hello spark", 3),
      ("hello spark scala", 2),
      ("hello spark scala hive", 1)
    )
    //1.将字符串转换为单词
   val newList: List[String] = dataList.map(s=>s._1.concat(" ") * s._2)
    val wordList: List[String] = newList.flatMap(_.split(" "))
    //2.将字符串分组
    val groupMap: Map[String, List[String]] = wordList.groupBy(word=>word)
    //3.统计相同单词的个数
    val wordCountMap: Map[String, Int] = groupMap.map(kv=>(kv._1,kv._2.length))
    //4.排序,降序
    val sortList: List[(String, Int)] = wordCountMap.toList.sortBy(kv=>kv._2)(Ordering.Int.reverse)
    //5.取前三名
    val result: List[(String, Int)] = sortList.take(3)
    println(result)



  }

}
