package com.atguigu.scala.day08

/**
 * @author zxjgreat
 * @create 2020-05-28 12:56
 */
object WordCount2 {

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
  def main(args: Array[String]): Unit = {
    val dataList = List(
      ("hello", 4),
      ("hello spark", 3),
      ("hello spark scala", 2),
      ("hello spark scala hive", 1)
    )
    //1.将字符串拆分成键值对
    def transform(s:(String,Int)) ={

      //按空格将字符串拆分成单词，返回单词的集合
      val array: Array[String] = s._1.split(" ")

      //返回字符串出现个数的集合
      for(i <- array) yield {
        (i,s._2)
      }
    }
    val wordList: List[(String, Int)] = dataList.flatMap(transform)
    //2.分组
    val groupMap: Map[String, List[(String, Int)]] = wordList.groupBy(kv=>kv._1)
    //3.求出相同单词的个数
    def trans(m : (String, List[(String, Int)])) ={
      val list: List[(String, Int)] = m._2
      var sum = 0
      for (i <- list){
        sum += i._2
      }
      (m._1,sum)
    }
    val wordCountMap: Map[String, Int] = groupMap.map(trans)
    //4.排序
    val sortList: List[(String, Int)] = wordCountMap.toList.sortBy(kv=>kv._2)(Ordering.Int.reverse)
    //5.取前三
    val result: List[(String, Int)] = sortList.take(3)
    //6.打印结果
    println(result)
















  }

}
